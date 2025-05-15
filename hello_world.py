#!/usr/bin/env python3

import sys
import utils

def main():
    """Enhanced hello world function with multiple features"""
    print("=" * 50)
    print("Welcome to the Interactive Python Demo!")
    print("=" * 50)
    
    name = input("What is your name? ")
    print(f"Nice to meet you, {name}!")
    
    # Demonstrate some basic Python features
    # Conditional statements
    if len(name) > 10:
        print(f"You have a long name with {len(name)} characters!")
    else:
        print(f"Your name has {len(name)} characters.")
        
    # Menu system
    while True:
        print("\nWhat would you like to do?")
        print("1. Count to a number")
        print("2. Check if a word is a palindrome")
        print("3. Generate a random password")
        print("4. Calculate a Fibonacci number")
        print("5. Exit")
        
        choice = input("\nEnter your choice (1-5): ")
        
        if choice == "1":
            try:
                count_to = int(input("Count up to what number? "))
                if count_to > 100:
                    print("That's a bit high. Let's limit to 100.")
                    count_to = 100
                print(f"\nCounting to {count_to}:")
                for i in range(1, count_to + 1):
                    print(f"Number {i}", end=" ")
                    if i % 10 == 0:
                        print()  # Line break every 10 numbers
                print()  # Final line break
            except ValueError:
                print("Please enter a valid number.")
                
        elif choice == "2":
            word = input("Enter a word to check if it's a palindrome: ")
            if utils.is_palindrome(word):
                print(f"'{word}' is a palindrome!")
            else:
                print(f"'{word}' is not a palindrome.")
                
        elif choice == "3":
            length = input("Password length (press Enter for default 12): ")
            special = input("Include special characters? (y/n, default: y): ").lower() != 'n'
            
            try:
                if length:
                    length = int(length)
                    if length < 4:
                        print("Password too short. Using minimum length of 4.")
                        length = 4
                else:
                    length = 12
                    
                password = utils.generate_random_password(length, special)
                print(f"\nYour random password is: {password}")
                print("(This is just for demonstration - don't use generated passwords in real applications)")
                
            except ValueError:
                print("Please enter a valid number for length.")
                
        elif choice == "4":
            try:
                n = int(input("Which Fibonacci number do you want to calculate? "))
                if n > 35:
                    print("Warning: Large values might take a while to calculate. Limiting to 35.")
                    n = 35
                    
                result, time_taken = utils.time_function(utils.calculate_fibonacci, n)
                print(f"The {n}th Fibonacci number is: {result}")
                print(f"Calculation took {time_taken:.6f} seconds")
                
            except ValueError:
                print("Please enter a valid number.")
                
        elif choice == "5":
            print(f"\nThank you for trying this Python demo, {name}! Goodbye!")
            sys.exit(0)
            
        else:
            print("Invalid choice. Please enter a number between 1 and 5.")

if __name__ == "__main__":
    main()
