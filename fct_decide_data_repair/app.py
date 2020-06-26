def lambda_handler(event, context):
    repair_settings = event["repair_settings"]

    repair_settings["attempt_count"] += 1
    repair_settings["attempt_repair"] = repair_settings["attempt_count"] <= repair_settings["attempts_limit"]

    return repair_settings
