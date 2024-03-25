# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Install GCC and other build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    build-essential \
    libatlas-base-dev \
    liblapack-dev \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install required Python packages
COPY requirements.txt ./
RUN pip install --upgrade pip setuptools wheel && \
    pip install cython numpy && \ 
    pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /usr/src/app
COPY . .

# Run train.py when the container launches
ENTRYPOINT ["python", "Train_SVD.py"]
