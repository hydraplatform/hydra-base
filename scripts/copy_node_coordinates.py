import hydra_base as hb
from hydra_base.db.model import Network, Node

def run():
    hb.db.connect()
    networks = hb.db.DBSession.query(Network).filter(Network.status != 'X', Network.projection != None).all()
    for network in networks:
        nodes = hb.db.DBSession.query(Node).filter(Node.network_id==network.id, Node.status != 'X').all()
        for node in nodes:
            print(f"Node {node.name} ({node.x}, {node.y})")

if __name__=="__main__":
    run()