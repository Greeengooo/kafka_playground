class Customer:
    def __init__(self, customer_id: int, name: str):
        self._customer_id = customer_id
        self._name = name

    @property
    def customer_id(self):
        return self._customer_id

    @property
    def name(self):
        return self._name

    def __str__(self):
        return f"Customer id:{self._customer_id}  name:{self._name}"
