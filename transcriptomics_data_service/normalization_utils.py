import pandas as pd
import numpy as np
from joblib import Parallel, delayed
from pandas.core.indexes.base import Index


def filter_counts(counts_df: pd.DataFrame):
    """Filter out genes (rows) and samples (columns) with zero total counts."""
    row_filter = counts_df.sum(axis=1) > 0
    col_filter = counts_df.sum(axis=0) > 0
    return counts_df.loc[row_filter, col_filter]


def prepare_counts_and_lengths(counts_df: pd.DataFrame, gene_lengths: pd.Series, scale_length: float = None):
    """Align counts and gene_lengths, drop zeros, and optionally scale gene lengths."""
    counts_df = counts_df.loc[gene_lengths.index]
    valid_lengths = gene_lengths.replace(0, pd.NA).dropna()
    counts_df = counts_df.loc[valid_lengths.index]
    gene_lengths = valid_lengths
    if scale_length is not None:
        gene_lengths = gene_lengths / scale_length
    return filter_counts(counts_df), gene_lengths


def parallel_apply(columns: Index, func, n_jobs=-1) -> pd.DataFrame:
    """Apply a function to each column in parallel and combine results."""
    results = Parallel(n_jobs=n_jobs)(delayed(func)(col) for col in columns)
    return pd.concat(results, axis=1)


def trim_values(log_ratio, log_mean, w, logratio_trim, sum_trim):
    """Perform log ratio and sum trimming."""
    n = len(log_ratio)
    loL = int(np.floor(n * logratio_trim / 2))
    hiL = n - loL
    lr_order = np.argsort(log_ratio)
    trimmed_idx = lr_order[loL:hiL]

    lr_t = log_ratio[trimmed_idx]
    w_t = w[trimmed_idx]
    mean_t = log_mean[trimmed_idx]

    n_t = len(mean_t)
    loS = int(np.floor(n_t * sum_trim / 2))
    hiS = n_t - loS
    mean_order = np.argsort(mean_t)
    final_idx = mean_order[loS:hiS]

    return lr_t[final_idx], w_t[final_idx]


def compute_TMM_normalization_factors(
    counts_df: pd.DataFrame, logratio_trim=0.3, sum_trim=0.05, weighting=True, n_jobs=-1
):
    """Compute TMM normalization factors for counts data."""
    lib_sizes = counts_df.sum(axis=0)
    median_lib = lib_sizes.median()
    ref_sample = (lib_sizes - median_lib).abs().idxmin()

    ref_counts = counts_df[ref_sample].values
    sample_names = counts_df.columns
    data_values = counts_df.values

    norm_factors = pd.Series(index=sample_names, dtype="float64")
    norm_factors[ref_sample] = 1.0

    def compute_norm_factor(sample):
        if sample == ref_sample:
            return sample, 1.0

        i = sample_names.get_loc(sample)
        data_i = data_values[:, i]

        mask = (data_i > 0) & (ref_counts > 0)
        data_i_masked = data_i[mask]
        data_r_masked = ref_counts[mask]

        N_i = data_i_masked.sum()
        N_r = data_r_masked.sum()

        data_i_norm = data_i_masked / N_i
        data_r_norm = data_r_masked / N_r

        log_ratio = np.log2(data_i_norm) - np.log2(data_r_norm)
        log_mean = 0.5 * (np.log2(data_i_norm) + np.log2(data_r_norm))

        w = 1.0 / (data_i_norm + data_r_norm) if weighting else np.ones_like(log_ratio)

        lr_final, w_final = trim_values(log_ratio, log_mean, w, logratio_trim, sum_trim)

        mean_M = np.sum(w_final * lr_final) / np.sum(w_final)
        norm_factor = 2**mean_M
        return sample, norm_factor

    samples = [s for s in sample_names if s != ref_sample]
    results = Parallel(n_jobs=n_jobs)(delayed(compute_norm_factor)(s) for s in samples)

    for sample, nf in results:
        norm_factors[sample] = nf

    norm_factors = norm_factors / np.exp(np.mean(np.log(norm_factors)))
    return norm_factors


def tmm_normalization(counts_df: pd.DataFrame, logratio_trim=0.3, sum_trim=0.05, weighting=True, n_jobs=-1):
    """Perform TMM normalization on counts data."""
    counts_df = filter_counts(counts_df)
    norm_factors = compute_TMM_normalization_factors(counts_df, logratio_trim, sum_trim, weighting, n_jobs)
    lib_sizes = counts_df.sum(axis=0)
    normalized_data = counts_df.div(lib_sizes, axis=1).div(norm_factors, axis=1) * lib_sizes.mean()
    return normalized_data


def getmm_normalization(
    counts_df: pd.DataFrame,
    gene_lengths: pd.Series,
    logratio_trim=0.3,
    sum_trim=0.05,
    scaling_factor=1e3,
    weighting=True,
    n_jobs=-1,
):
    """Perform GeTMM normalization on counts data."""
    counts_df, gene_lengths = prepare_counts_and_lengths(counts_df, gene_lengths)
    rpk = counts_df.mul(scaling_factor).div(gene_lengths, axis=0)
    return tmm_normalization(rpk, logratio_trim, sum_trim, weighting, n_jobs)


def compute_rpk(counts_df: pd.DataFrame, gene_lengths_scaled: pd.Series, n_jobs=-1):
    """Compute RPK values in parallel."""
    columns = counts_df.columns

    def rpk_col(col):
        return counts_df[col] / gene_lengths_scaled

    rpk = parallel_apply(columns, rpk_col, n_jobs)
    rpk.columns = columns
    return rpk


def tpm_normalization(counts_df: pd.DataFrame, gene_lengths: pd.Series, scale_library=1e6, scale_length=1e3, n_jobs=-1):
    """Convert raw read counts to TPM in parallel."""
    counts_df, gene_lengths_scaled = prepare_counts_and_lengths(counts_df, gene_lengths, scale_length=scale_length)
    rpk = compute_rpk(counts_df, gene_lengths_scaled, n_jobs)
    scaling_factors = rpk.sum(axis=0).replace(0, pd.NA)
    scaling_factors_norm = scaling_factors / scale_library

    def tpm_col(col):
        return rpk[col] / scaling_factors_norm[col]

    tpm = parallel_apply(rpk.columns, tpm_col, n_jobs)
    tpm.columns = rpk.columns
    return tpm
