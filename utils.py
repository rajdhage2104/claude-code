#!/usr/bin/env python3

"""
Utility functions for common tasks
"""

import os
import random
import time
import string
from datetime import datetime


def generate_random_number(min_val=1, max_val=100):
    """Generate a random number between min_val and max_val
    
    Args:
        min_val (int): Minimum value (inclusive)
        max_val (int): Maximum value (inclusive)
        
    Returns:
        int: Random number
    """
    return random.randint(min_val, max_val)


def get_current_datetime():
    """Get the current date and time formatted as a string
    
    Returns:
        str: Current date and time
    """
    now = datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S")


def calculate_fibonacci(n):
    """Calculate the nth Fibonacci number
    
    Args:
        n (int): The position in the Fibonacci sequence
        
    Returns:
        int: The nth Fibonacci number
    """
    if n <= 0:
        return 0
    elif n == 1:
        return 1
    else:
        a, b = 0, 1
        for _ in range(2, n + 1):
            a, b = b, a + b
        return b


def is_palindrome(text):
    """Check if the given text is a palindrome
    
    Args:
        text (str): The text to check
        
    Returns:
        bool: True if the text is a palindrome, False otherwise
    """
    # Remove spaces and convert to lowercase
    text = text.lower().replace(" ", "")
    return text == text[::-1]


def time_function(func, *args, **kwargs):
    """Measure the execution time of a function
    
    Args:
        func (callable): The function to time
        *args: Arguments to pass to the function
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        tuple: (function result, execution time in seconds)
    """
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    execution_time = end_time - start_time
    return result, execution_time


def generate_random_password(length=12, include_special=True):
    """Generate a random password with the specified length
    
    Args:
        length (int): Length of the password to generate
        include_special (bool): Whether to include special characters
        
    Returns:
        str: Random password
    """
    # Define character sets
    letters = string.ascii_letters
    digits = string.digits
    special_chars = string.punctuation if include_special else ""
    
    # Combine all characters
    all_chars = letters + digits + special_chars
    
    # Ensure we have at least one of each type
    password = [
        random.choice(letters),
        random.choice(digits)
    ]
    
    if include_special:
        password.append(random.choice(special_chars))
    
    # Fill the rest randomly
    password.extend(random.choice(all_chars) for _ in range(length - len(password)))
    
    # Shuffle the password
    random.shuffle(password)
    
    return ''.join(password)


# Example usage
if __name__ == "__main__":
    print(f"Random number: {generate_random_number(1, 10)}")
    print(f"Current datetime: {get_current_datetime()}")
    print(f"10th Fibonacci number: {calculate_fibonacci(10)}")
    print(f"Is 'racecar' a palindrome? {is_palindrome('racecar')}")
    print(f"Is 'hello' a palindrome? {is_palindrome('hello')}")
    
    # Time a function
    result, time_taken = time_function(calculate_fibonacci, 30)
    print(f"Result: {result}, Time taken: {time_taken:.6f} seconds")
    
    # Generate a random password
    password = generate_random_password()
    print(f"Random password: {password}")
