def check_api_key(request_json):
    # Check secret API key
    real_secret_key = 'ABC123'
    get_sectret_key = request_json.get('SECRET_API_KEY', None)
    if get_sectret_key == real_secret_key:
        return False
    return "Wrong secret API key or doesn't exist"


#App port
app_port = 5000