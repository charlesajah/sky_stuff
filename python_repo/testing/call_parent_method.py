class ParentClass:
    def parent_method(self):
        print("This is a method from the parent class")

class ChildClass(ParentClass):
    def child_method(self):
       
        print("This is a method from the child class")
        super().parent_method()  # Call the parent method using super()

child = ChildClass()
child.child_method()

