class Flight:
    def __init__(self, json: dict) -> None:
        self.flight_date = json["flight_date"]
        self.diata = json["departure"]["iata"]
        self.aiata = json["arrival"]["iata"]
        self.airline = json["airline"]["name"]
        self.aircraft = json["aircraft"]

    def to_json(self) -> dict:
        return self.__dict__
