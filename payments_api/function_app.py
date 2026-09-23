import azure.functions as func
import logging
import json
import os

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="get_payments")
def get_payments(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Payments API called.")

    try:
        # Step 1: find the JSON file sitting next to this code file
        folder = os.path.dirname(__file__)
        file_path = os.path.join(folder, "payments_api.json")

        # Step 2: open the file and load it into Python
        with open(file_path, "r") as f:
            data = json.load(f)

        # Step 3: send it back as a JSON response
        return func.HttpResponse(
            json.dumps(data),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        # Step 4: if anything goes wrong, return an error instead of crashing
        logging.error(f"Failed to load payments data: {e}")
        return func.HttpResponse(
            "Error loading payments data.",
            status_code=500
        )