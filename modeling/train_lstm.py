#!/usr/bin/env python3
"""
train_lstm.py — joint multi-asset LSTM with per-asset embedding.

Architecture:

    asset_idx ──► nn.Embedding(6, 8) ─┐
                                       ├─► LSTM(input=4+8, hidden=64,        ─► last hidden
    feature_seq ─────────────────────┘   layers=2, dropout=0.2, batch_first=True)
                                                                              │
                                                                              ▼
                                                                          Linear → ŷ (log return)

Reads per-asset 1-second tables from `data/training_lstm/{asset}.parquet`
(produced by `build-dataset-lstm`), builds 60-step rolling sequences, and
trains a joint model on all 6 assets simultaneously. Standardization is
fit on the training portion only and persisted in `data/lstm_meta.json`
so the live scorer uses identical normalization.

Device: cuda → mps → cpu, picked automatically. RTX 2060 / Apple M-series
both work; CPU works too but slowly.

Usage:
    train-lstm
    train-lstm --epochs 30 --batch-size 256 --hidden 96
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from sklearn.metrics import mean_absolute_error, mean_squared_error
from torch.utils.data import DataLoader, Dataset

ASSETS_DEFAULT = ["btc", "eth", "sol", "xrp", "bnb", "doge"]
FEATURE_COLS = ["log_return_1s", "poly_p_up", "poly_p_change_1s", "vol_60s"]
TARGET_COL = "log_return_5m_target"


# ------------------------------------------------------------ data loading

@dataclass
class AssetSplit:
    asset: str
    asset_idx: int
    train_arr: np.ndarray   # (T_train, F)
    train_target: np.ndarray
    test_arr: np.ndarray
    test_target: np.ndarray


def load_per_asset_tables(
    training_dir: Path, assets: list[str], holdout_frac: float
) -> tuple[dict[str, AssetSplit], np.ndarray, np.ndarray]:
    """Load each asset's 1-s table and split per-asset by time. Compute
    train-only mean/std for standardization (no leakage)."""
    asset_to_idx = {a: i for i, a in enumerate(assets)}
    splits: dict[str, AssetSplit] = {}
    train_arrays: list[np.ndarray] = []

    for asset in assets:
        path = training_dir / f"{asset}.parquet"
        if not path.exists():
            print(f"  {asset}: missing {path} — skipping")
            continue
        df = pd.read_parquet(path).sort_index()
        if len(df) < 200:
            print(f"  {asset}: only {len(df)} rows — skipping (too short to train)")
            continue

        cutoff = int(len(df) * (1 - holdout_frac))
        train_arr = df[FEATURE_COLS].iloc[:cutoff].to_numpy(dtype=np.float32)
        train_target = df[TARGET_COL].iloc[:cutoff].to_numpy(dtype=np.float32)
        test_arr = df[FEATURE_COLS].iloc[cutoff:].to_numpy(dtype=np.float32)
        test_target = df[TARGET_COL].iloc[cutoff:].to_numpy(dtype=np.float32)

        splits[asset] = AssetSplit(
            asset=asset,
            asset_idx=asset_to_idx[asset],
            train_arr=train_arr,
            train_target=train_target,
            test_arr=test_arr,
            test_target=test_target,
        )
        train_arrays.append(train_arr)
        print(
            f"  {asset:<5}  total={len(df):,}  "
            f"train={len(train_arr):,}  holdout={len(test_arr):,}"
        )

    if not splits:
        raise SystemExit("no assets loaded — run build-dataset-lstm first")

    stacked = np.concatenate(train_arrays, axis=0)
    means = stacked.mean(axis=0).astype(np.float32)
    stds = stacked.std(axis=0).astype(np.float32) + 1e-8
    return splits, means, stds


class SequenceDataset(Dataset):
    """Sliding-window sequences across all loaded assets, pre-standardized."""

    def __init__(
        self,
        splits: dict[str, AssetSplit],
        seq_len: int,
        means: np.ndarray,
        stds: np.ndarray,
        which: str,
    ):
        assert which in ("train", "test")
        self.seq_len = seq_len
        self.means = means
        self.stds = stds

        self._examples: list[tuple[int, int, int]] = []
        self._arrays: list[np.ndarray] = []
        self._targets: list[np.ndarray] = []
        self._asset_idx: list[int] = []

        for split in splits.values():
            arr = split.train_arr if which == "train" else split.test_arr
            target = split.train_target if which == "train" else split.test_target
            if len(arr) <= seq_len:
                continue
            arr_norm = (arr - means) / stds
            ai = len(self._arrays)
            self._arrays.append(arr_norm.astype(np.float32))
            self._targets.append(target.astype(np.float32))
            self._asset_idx.append(split.asset_idx)
            for start in range(len(arr_norm) - seq_len):
                self._examples.append((ai, start, split.asset_idx))

    def __len__(self) -> int:
        return len(self._examples)

    def __getitem__(self, i: int):
        ai, start, asset_idx = self._examples[i]
        end = start + self.seq_len
        x = self._arrays[ai][start:end]
        y = self._targets[ai][end - 1]
        return torch.from_numpy(x), torch.tensor(asset_idx, dtype=torch.long), torch.tensor(y)


# ------------------------------------------------------------ model

class JointAssetLSTM(nn.Module):
    def __init__(
        self,
        num_assets: int,
        embed_dim: int,
        num_features: int,
        hidden: int,
        num_layers: int,
        dropout: float,
    ) -> None:
        super().__init__()
        self.embed = nn.Embedding(num_assets, embed_dim)
        self.lstm = nn.LSTM(
            input_size=num_features + embed_dim,
            hidden_size=hidden,
            num_layers=num_layers,
            dropout=dropout if num_layers > 1 else 0.0,
            batch_first=True,
        )
        self.head = nn.Linear(hidden, 1)

    def forward(
        self, x: torch.Tensor, asset_idx: torch.Tensor,
        h0: torch.Tensor | None = None, c0: torch.Tensor | None = None,
    ) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        # x: (B, T, F), asset_idx: (B,)
        B, T, _ = x.shape
        emb = self.embed(asset_idx).unsqueeze(1).expand(-1, T, -1)  # (B, T, E)
        x_aug = torch.cat([x, emb], dim=-1)
        if h0 is None or c0 is None:
            out, (h, c) = self.lstm(x_aug)
        else:
            out, (h, c) = self.lstm(x_aug, (h0, c0))
        return self.head(out[:, -1, :]).squeeze(-1), h, c


# ------------------------------------------------------------ training loop

def pick_device(prefer: str = "auto") -> torch.device:
    """Auto: cuda → cpu (MPS skipped — has reproducible NaN issues with LSTM
    in current PyTorch builds; we'd rather train slowly on CPU than get NaN
    loss). The user can force MPS with `--device mps` if they want to try."""
    if prefer == "auto":
        if torch.cuda.is_available():
            return torch.device("cuda")
        return torch.device("cpu")
    return torch.device(prefer)


def report(name: str, y_true: np.ndarray, y_pred: np.ndarray) -> None:
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mae = float(mean_absolute_error(y_true, y_pred))
    nz = y_true != 0
    if nz.any():
        dir_acc = float(np.mean(np.sign(y_pred[nz]) == np.sign(y_true[nz])))
    else:
        dir_acc = float("nan")
    print(
        f"  {name:<28}  "
        f"RMSE={rmse * 10_000:6.2f} bps  "
        f"MAE={mae * 10_000:6.2f} bps  "
        f"dir_acc={dir_acc * 100:5.1f}%"
    )


def evaluate(
    model: JointAssetLSTM, loader: DataLoader, device: torch.device
) -> tuple[np.ndarray, np.ndarray]:
    model.eval()
    preds: list[np.ndarray] = []
    targets: list[np.ndarray] = []
    with torch.no_grad():
        for x, a, y in loader:
            x = x.to(device, non_blocking=True)
            a = a.to(device, non_blocking=True)
            p, _, _ = model(x, a)
            preds.append(p.cpu().numpy())
            targets.append(y.numpy())
    return np.concatenate(targets), np.concatenate(preds)


def main() -> None:
    parser = argparse.ArgumentParser(description="Train joint multi-asset LSTM")
    parser.add_argument("--training-dir", default="data/training_lstm")
    parser.add_argument("--model-out", default="data/model.lstm.pt")
    parser.add_argument("--meta-out", default="data/lstm_meta.json")
    parser.add_argument("--assets", nargs="+", default=ASSETS_DEFAULT)
    parser.add_argument("--seq-len", type=int, default=60)
    parser.add_argument("--embed-dim", type=int, default=8)
    parser.add_argument("--hidden", type=int, default=64)
    parser.add_argument("--num-layers", type=int, default=2)
    parser.add_argument("--dropout", type=float, default=0.2)
    parser.add_argument("--epochs", type=int, default=15)
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--learning-rate", type=float, default=1e-3)
    parser.add_argument("--weight-decay", type=float, default=1e-4)
    parser.add_argument("--holdout-frac", type=float, default=0.2)
    parser.add_argument("--num-workers", type=int, default=0)
    parser.add_argument(
        "--device",
        default="auto",
        choices=["auto", "cuda", "mps", "cpu"],
        help="Auto: cuda → cpu (MPS skipped due to LSTM NaN issues; force with --device mps to test)",
    )
    args = parser.parse_args()

    device = pick_device(args.device)
    print(f"device: {device}")

    splits, means, stds = load_per_asset_tables(
        Path(args.training_dir), args.assets, args.holdout_frac
    )

    train_ds = SequenceDataset(splits, args.seq_len, means, stds, "train")
    test_ds = SequenceDataset(splits, args.seq_len, means, stds, "test")
    if len(train_ds) == 0 or len(test_ds) == 0:
        raise SystemExit(
            f"not enough sequences (train={len(train_ds)}, holdout={len(test_ds)}); "
            f"collect more data with run-overnight.sh and rebuild"
        )
    print(f"sequences  train={len(train_ds):,}  holdout={len(test_ds):,}")

    train_loader = DataLoader(
        train_ds, batch_size=args.batch_size, shuffle=True,
        num_workers=args.num_workers, pin_memory=(device.type == "cuda"),
    )
    test_loader = DataLoader(
        test_ds, batch_size=args.batch_size, shuffle=False,
        num_workers=args.num_workers, pin_memory=(device.type == "cuda"),
    )

    model = JointAssetLSTM(
        num_assets=len(args.assets),
        embed_dim=args.embed_dim,
        num_features=len(FEATURE_COLS),
        hidden=args.hidden,
        num_layers=args.num_layers,
        dropout=args.dropout,
    ).to(device)
    n_params = sum(p.numel() for p in model.parameters())
    print(f"model params: {n_params:,}")

    optim = torch.optim.AdamW(model.parameters(), lr=args.learning_rate, weight_decay=args.weight_decay)
    loss_fn = nn.MSELoss()

    print("\ntraining:")
    for epoch in range(1, args.epochs + 1):
        model.train()
        t0 = time.time()
        total_loss = 0.0
        n_batches = 0
        for x, a, y in train_loader:
            x = x.to(device, non_blocking=True)
            a = a.to(device, non_blocking=True)
            y = y.to(device, non_blocking=True)
            optim.zero_grad()
            p, _, _ = model(x, a)
            loss = loss_fn(p, y)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optim.step()
            total_loss += float(loss.item())
            n_batches += 1
        elapsed = time.time() - t0
        print(f"  epoch {epoch:>2}  train_mse={total_loss / n_batches:.6e}  ({elapsed:.1f}s)")

    print("\nholdout metrics overall:")
    y_train_true, y_train_pred = evaluate(model, train_loader, device)
    y_test_true, y_test_pred = evaluate(model, test_loader, device)
    report("model · train", y_train_true, y_train_pred)
    report("model · holdout", y_test_true, y_test_pred)

    print("\nholdout metrics per asset:")
    asset_to_idx = {a: i for i, a in enumerate(args.assets)}
    for asset, idx in asset_to_idx.items():
        if asset not in splits:
            continue
        # Walk the test_ds._examples to slice per-asset predictions
        mask = np.array([ex[2] == idx for ex in test_ds._examples])
        if not mask.any():
            continue
        report(f"holdout · {asset.upper()}", y_test_true[mask], y_test_pred[mask])

    out_model = Path(args.model_out)
    out_meta = Path(args.meta_out)
    out_model.parent.mkdir(parents=True, exist_ok=True)
    torch.save(model.state_dict(), out_model)
    out_meta.write_text(json.dumps(
        {
            "assets": args.assets,
            "asset_to_idx": asset_to_idx,
            "feature_cols": FEATURE_COLS,
            "target_col": TARGET_COL,
            "seq_len": args.seq_len,
            "embed_dim": args.embed_dim,
            "hidden": args.hidden,
            "num_layers": args.num_layers,
            "dropout": args.dropout,
            "feature_means": means.tolist(),
            "feature_stds": stds.tolist(),
            "device": str(device),
            "n_train_sequences": len(train_ds),
            "n_holdout_sequences": len(test_ds),
        },
        indent=2,
    ))
    print(f"\nsaved {out_model}, {out_meta}")


if __name__ == "__main__":
    main()
