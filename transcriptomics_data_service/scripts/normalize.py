import pandas as pd


def read_counts2tpm(counts_df, gene_lengths, scale_library=1e6, scale_length=1e3):
    """
    Convert raw read counts to TPM (Transcripts Per Million).

    Parameters:
    counts_df (DataFrame): DataFrame with genes as rows and samples as columns.
    gene_lengths (Series): Series with gene lengths, index matches counts_df.index.
    scale_library (int or float): Scaling factor for library size normalization (default 1e6).
    scale_length (int or float): Scaling factor for gene length scaling (default 1e3).

    Returns:
    DataFrame: TPM-normalized values.
    """
    # Ensure counts_df and gene_lengths are aligned
    counts_df = counts_df.loc[gene_lengths.index]

    # Scale gene lengths
    gene_lengths_scaled = gene_lengths / scale_length

    # Calculate Reads Per Scaled Kilobase (RPK)
    rpk = counts_df.div(gene_lengths_scaled, axis=0)

    # Calculate scaling factors
    scaling_factors = rpk.sum(axis=0) / scale_library

    # Calculate TPM
    tpm = rpk.div(scaling_factors, axis=1)

    return tpm


def tmm_normalization(counts_df):
    """
    Perform TMM normalization on counts data.

    Parameters:
    counts_df (DataFrame): DataFrame with genes as rows and samples as columns.

    Returns:
    DataFrame: TMM-normalized values.
    """
    try:
        import conorm
    except ImportError:
        raise ImportError("The 'conorm' package is required for this function but is not installed.")
    normalized_array = conorm.tmm(counts_df)
    normalized_df = pd.DataFrame(normalized_array, columns=counts_df.columns, index=counts_df.index)
    return normalized_df


def getmm_normalization(counts_df, gene_lengths):
    """
    Perform GeTMM normalization on counts data.

    Parameters:
    counts_df (DataFrame): DataFrame with genes as rows and samples as columns.
    gene_lengths (Series): Series with gene lengths, index matches counts_df.index.

    Returns:
    DataFrame: GeTMM-normalized values.
    """
    try:
        import conorm
    except ImportError:
        raise ImportError("The 'conorm' package is required for this function but is not installed.")

    normalized_array = conorm.getmm(counts_df, gene_lengths)
    normalized_df = pd.DataFrame(normalized_array, columns=counts_df.columns, index=counts_df.index)
    return normalized_df
