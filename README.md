# Project Overview

This project involves building a data processing pipeline with **Apache Spark** for a video streaming platform using a MovieLens dataset. The dataset includes movies, TV series, their genres, and user ratings.

## Key Components

### Apache Spark

Utilizing **Apache Spark**, this project focuses on:
- Efficient data management and pre-processing.
- Performing analytics to support a recommendation system.

### Features

*   **Smart Recommendations**: The app analyzes your viewing history and preferences to suggest similar movies and shows. It uses techniques like **Locality-Sensitive Hashing** to identify titles with similar themes or genres and **Collaborative Filtering** to consider recommendations from other users with similar tastes.
    
*   **Average Ratings**: Each movie and show displays an average rating based on user feedback, helping you make informed choices about what to watch.
    
*   **Similar Titles**: When you select a movie, the app provides a list of other titles that share similar keywords, making it easy to explore related content.

### Project Workflow

The project consists of three milestones:
1. **Data Analysis**: Load and analyze the dataset to understand user ratings and movie genres.
2. **Movie-Ratings Pipeline**: Aggregate user ratings to calculate average ratings for each title.
3. **Prediction Serving**: Develop a recommendation system that suggests similar movies based on user preferences and interactions. This feature allows the application to provide personalized recommendations, enhancing user experience by helping them discover new content.

## Summary

The primary goal is to enable the application to efficiently serve recommendations and statistics, making it easier for users to find movies and TV shows they’ll enjoy.
