def rename_subjob_args(arg_mapping: dict, current_args) -> dict:
    """Rename subjob arguments based on a provided mapping.
        This allows us to pass many arguments from the main job and specify which ones to use for each subjob,
        even if the argument names differ between the main job and subjobs.
    Args:
        arg_mapping (dict): Dictionary mapping current argument names to new names.
        current_args: Current arguments object (e.g., argparse.extract_input_file).
    Returns:
        dict: Dictionary with renamed arguments.
    """
    renamed_args = {}

    # Iterate through the mapping and rename arguments
    for old_name, new_name in arg_mapping.items():
        if old_name in current_args:
            renamed_args[new_name] = current_args[old_name]
    return renamed_args
