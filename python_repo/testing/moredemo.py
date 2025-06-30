

class ParentClass:
    def __init__(self, name):
        self.name = name

class ChildClass(ParentClass):
    def __init__(self, name, age):
        #super().__init__(name)
        self.age = age

    def print_info(self):
        print(f"My name is {self.name} and I am {self.age} years old.")

child = ChildClass("Alice", 25)
child.print_info()
