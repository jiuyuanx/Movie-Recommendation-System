import requests


# Function to get movie information by movie_id
def get_movie_info(movie_id, raise_for_status = False):
    api_url = f"http://128.2.204.215:8080/movie/{movie_id}"
    
    try:
        response = requests.get(api_url)
        if raise_for_status:
            response.raise_for_status()
        # Check if the movie was found
        if response.status_code == 200:
            movie_info = response.json()
            # Handle the case where the movie is not found
            return movie_info
        else:
            print(f"Failed to fetch movie with ID {movie_id}. HTTP Status Code: {response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"An error occurred while fetching movie with ID {movie_id}: {e}")
        return None

# Function to get user information by user_id
def get_user_info(user_id, raise_for_status = False):
    api_url = f"http://128.2.204.215:8080/user/{user_id}"
    
    try:
        response = requests.get(api_url)
        if raise_for_status:
            response.raise_for_status()
        # Check if the user was found
        if response.status_code == 200:
            user_info = response.json()
            # Handle the case where the user is not found
            return user_info
        else:
            print(f"Failed to fetch user with ID {user_id}. HTTP Status Code: {response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"An error occurred while fetching user with ID {user_id}: {e}")
        return None