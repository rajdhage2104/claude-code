#!/usr/bin/env python3

def main():
    """Simple hello world function"""
    print("Hello, World!")
    name = input("What is your name? ")
    print(f"Nice to meet you, {name}!")
    
    # Demonstrate some basic Python features
    # Conditional statements
    if len(name) > 10:
        print("You have a long name!")
    else:
        print("Your name is quite short.")
        
    # Loop example
    print("\nCounting to 5:")
    for i in range(1, 6):
        print(f"Number {i}")
        
    print("\nThank you for trying this basic Python script!")

if __name__ == "__main__":
    main()
