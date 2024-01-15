#!/bin/bash

# Define the base directory where you want to create subdirectories
base_directory="./"

# Create the subdirectories
mkdir -p "$base_directory/storage/aggregated_zone"
mkdir -p "$base_directory/storage/cache"
mkdir -p "$base_directory/storage/cleansed_zone"
mkdir -p "$base_directory/storage/landing_zone"
mkdir -p "$base_directory/airflow/logs"

# Make this script executable
chmod +x "$0"

echo "Directories created and script is now executable."