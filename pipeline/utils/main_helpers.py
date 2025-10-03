def rename_subjob_args(arg_mapping: dict, current_args) -> dict:
    """Rename subjob arguments based on a provided mapping.
    Args:
        arg_mapping (dict): Dictionary mapping current argument names to new names.
        current_args: Current arguments object (e.g., argparse.extract_input_file).
    Returns:
        dict: Dictionary with renamed arguments.
    """
    renamed_args = {}
    for k, v in arg_mapping.items():
        if k in vars(current_args):
            renamed_args[v] = getattr(current_args, k)
    return renamed_args
