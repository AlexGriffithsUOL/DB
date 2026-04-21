class Directions:
    LEFT = 'left'
    RIGHT = 'right'
    
    @classmethod
    def validate_direction(cls, value):
        if value not in (cls.LEFT, cls.RIGHT):
            raise Exception('Invalid direction')