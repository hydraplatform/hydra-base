"""
This is a utility which takes a source network ID and target network ID,
and sets the types from the source onto the target -- this is needed when
the resource types for the target network don't exist.

The names of the nodes in both networks need to be the same
"""

import click
import hydra_base as hb
from hydra_base.lib.objects import JSONObject
from hydra_base.db.model import *

class TypeRestorer:
    def __init__(self, source_network_id, target_network_id, default_link):
        """
            The default link is the default link to use if a link exists in the
            target network but not in the source

            We do not provide a default node argument, because the basic functionality
            relies on the nodes in the target being a subset of the source, and so no
            default should be necessary
        """
        self.source_network_id = source_network_id
        self.target_network_id = target_network_id

        self.source_network = None
        self.source_nodes = []
        self.source_links = []
        self.source_groups = []

        self.target_network = None
        self.target_nodes = []
        self.target_links = []
        self.target_groups = []

        self.network_type_map = {}
        self.node_type_map = {}
        self.link_type_map = {}
        self.group_type_map = {}

        self.template = None

        self.new_resource_types = {'NODE': 0, 'LINK': 0, 'GROUP': 0, 'NETWORK': 0}

        self.default_link_name = default_link
        self.default_link_type = None

    def load_data(self):

        hb.db.connect()

        self.source_network = hb.db.DBSession.query(Network).filter(Network.id == self.source_network_id).one()
        self.source_nodes = hb.db.DBSession.query(Node).filter(Node.network_id == self.source_network_id).all()
        self.source_links = hb.db.DBSession.query(Link).filter(Link.network_id == self.source_network_id).all()
        self.source_groups = hb.db.DBSession.query(ResourceGroup).filter(ResourceGroup.network_id == self.source_network_id).all()

        self.target_network = hb.db.DBSession.query(Network).filter(Network.id == self.target_network_id).one()
        self.target_nodes = hb.db.DBSession.query(Node).filter(Node.network_id == self.target_network_id).all()
        self.target_links = hb.db.DBSession.query(Link).filter(Link.network_id == self.target_network_id).all()
        self.target_groups = hb.db.DBSession.query(ResourceGroup).filter(ResourceGroup.network_id == self.target_network_id).all()

        self.template = self.source_network.types[0].templatetype.template

        if self.default_link_name is not None:
            for t in self.template.templatetypes:
                if t.name == self.default_link_name:
                    self.default_link_type = JSONObject({'type_id': t.id})

        # SOURCE NODES
        for n in self.source_nodes:
            self.node_type_map[n.name] = n.types[0]
        # SOURCE LINKS
        for l in self.source_links:
            start_node = l.node_a.name
            end_node   = l.node_b.name
            self.link_type_map[(start_node, end_node)] = l.types[0]
        # SOURCE GROUPS
        for g in self.source_groups:
            self.group_type_map[g.name] = g.types[0]

    # generic function to add a resourcetype
    def addResourceType(self, ref_key, target_resource, source_resource_type):
        for t in target_resource.types:
            if t.type_id == source_resource_type.type_id:
                print(f"{ref_key} {target_resource.name} already has the correct type")
                break
        else:
            rt = ResourceType()
            rt.ref_key = ref_key
            if ref_key == "GROUP":
                rt.group_id = target_resource.id
            elif ref_key == "NODE":
                rt.node_id = target_resource.id
            elif ref_key == "NETWORK":
                rt.network_id = target_resource.id
            elif ref_key == "LINK":
                rt.link_id = target_resource.id
            else:
                raise Exception(f"The ref_key {ref_key} is not valid!")

            rt.type_id = source_resource_type.type_id
            rt.child_template_id = source_resource_type.child_template_id
            self.new_resource_types[ref_key] = self.new_resource_types[ref_key] + 1
            hb.db.DBSession.add(rt)

    def check_networks_compatible(self):
        """
            Check the 2 networks are compatible by testing whether the source network
            contains all the nodes needed by the target network
        """
        #first check we've got compatible networks
        for n in self.target_nodes:
            if n.name not in self.node_type_map:
                raise Exception(f"Unable to map types. Found a node name '{n.name}' in"
                                f" target network {self.target_network_id} which is "
                                f"not in the source network {self.source_network_id}")

        #If a default link type is provided, it means that there are probably links the targtet
        #network not present in the source. If this is the case, then don't bother checking
        #for compatible links
        if self.default_link_type is None:
            for l in self.target_links:
                start_node = l.node_a.name
                end_node   = l.node_b.name
                if (start_node, end_node) not in self.link_type_map:
                    raise Exception(f"Unable to map types. Found a link name '{l.name}' in"
                                    f" target network {self.target_network_id} which is "
                                    f"not in the source network {self.source_network_id}")

        for g in self.target_groups:
            if g.name not in self.group_type_map:
                raise Exception(f"Unable to map types. Found a group name '{g.name}' in"
                                f" target network {self.target_network_id} which is "
                                f"not in the source network {self.source_network_id}")

    def run(self):
        self.check_networks_compatible()

        #OK we have compatible networks. Now create some resource types
        self.addResourceType("NETWORK",
                                   self.target_network,
                                   self.source_network.types[0])

        for n in self.target_nodes:
            matching_type = self.node_type_map[n.name]
            self.addResourceType("NODE", n, matching_type)


        for l in self.target_links:
            start_node = l.node_a.name
            end_node   = l.node_b.name
            matching_type = self.link_type_map.get((start_node, end_node), self.default_link_type)
            self.addResourceType("LINK", l, matching_type)

        for g in self.target_groups:
            matching_type = self.group_type_map[g.name]
            self.addResourceType("GROUP", g, matching_type)

        try:

            # To reenable later
            hb.db.DBSession.flush()
            hb.commit_transaction()
            #hb.rollback_transaction()
            print("Inserted types: %s"%self.new_resource_types)
        except Exception as e:
            print("An error has occurred: %s", e)
            hb.rollback_transaction()

@click.command()
@click.argument('source_network_id')
@click.argument('target_network_id')
@click.option('--default-link', default=None, help="Name of a default link type, if a link exists in the target but not the source")
def restore(source_network_id, target_network_id, default_link=None):
    restorer = TypeRestorer(source_network_id, target_network_id, default_link=default_link)
    restorer.load_data()
    restorer.run()

if __name__ == '__main__':
    restore()
