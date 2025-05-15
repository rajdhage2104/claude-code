#!/usr/bin/env python3

class Person:
    """A simple class to represent a person"""
    
    def __init__(self, name, age, occupation):
        """Initialize a new Person object
        
        Args:
            name (str): The person's name
            age (int): The person's age
            occupation (str): The person's occupation
        """
        self.name = name
        self.age = age
        self.occupation = occupation
        
    def greet(self):
        """Return a greeting string"""
        return f"Hello, my name is {self.name}. I am {self.age} years old and I work as a {self.occupation}."
    
    def have_birthday(self):
        """Increment the person's age by 1"""
        self.age += 1
        return f"Happy Birthday! {self.name} is now {self.age} years old."
    
    def change_job(self, new_occupation):
        """Change the person's occupation
        
        Args:
            new_occupation (str): The new occupation
        """
        old_occupation = self.occupation
        self.occupation = new_occupation
        return f"{self.name} changed jobs from {old_occupation} to {new_occupation}."

# Example usage
if __name__ == "__main__":
    # Create a new person
    alice = Person("Alice", 28, "Software Engineer")
    
    # Call methods on the person object
    print(alice.greet())
    print(alice.have_birthday())
    print(alice.change_job("Data Scientist"))
    print(alice.greet())
