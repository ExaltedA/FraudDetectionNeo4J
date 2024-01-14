class Config:
    def __init__(self,size) -> None:
        # See https://neo4j.com/developer/aura-connect-driver/ for Aura specific connection URL.
        self.Scheme = "neo4j"  # Connecting to Aura, use the "neo4j+s" or "neo4j+ssc" URI scheme
        self.Host_name = f"neo4j_{size}"
        self.Port = 7687
        self.Url = f"{self.Scheme}://{self.Host_name}:{self.Port}"
        
        self.User = "neo4j"
        self.Password = "neo4jpassword"