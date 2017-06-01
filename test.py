class C:
    def __init__(self):
        self._x = 1

    @property
    def x(self):
        """I'm the 'x' property."""
        return self._x

   
x = C()
print x.x