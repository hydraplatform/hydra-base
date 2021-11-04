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
    def __init__(self, source_network_id, type_csv, template_id, target_network_id, default_link, debug=False):
        """
            The default link is the default link to use if a link exists in the
            target network but not in the source

            We do not provide a default node argument, because the basic functionality
            relies on the nodes in the target being a subset of the source, and so no
            default should be necessary
        """
        self.source_network_id = source_network_id
        self.target_network_id = target_network_id
        self.type_csv = type_csv
        self.template_id = template_id

        self.check_inputs()

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
        self.network_type = None

        self.new_resource_types = {'NODE': 0, 'LINK': 0, 'GROUP': 0, 'NETWORK': 0}

        self.default_link_name = default_link
        self.default_link_type = None

        self.debug = debug # don't commit the transaction if true

    def check_inputs(self):
        """
            make sure the inputs are all valid
        """

        if self.source_network_id is None and self.type_csv is None:
            raise Exception("Please specifiy a '--source_network_id' or a '--type_csv'")

        if self.type_csv is not None and self.template_id is None:
            raise Exception("Cannot use --type-csv without a --template-id argument."
                            " Please specify a --template-id")

        if self.target_network_id is None:
            raise Exception("Please specify a target network ID with '--target_network_id'")

        print("Inputs are valid")

    def load_data(self):

        hb.db.connect()

        self.target_network = hb.db.DBSession.query(Network).filter(
            Network.id == self.target_network_id).one()
        self.target_nodes = hb.db.DBSession.query(Node).filter(
            Node.network_id == self.target_network_id).all()
        self.target_links = hb.db.DBSession.query(Link).filter(
            Link.network_id == self.target_network_id).all()
        self.target_groups = hb.db.DBSession.query(ResourceGroup).filter(
            ResourceGroup.network_id == self.target_network_id).all()

        self.load_type_map()
        self.set_default_link_type()


    def set_default_link_type(self):
        """
            Identify and set the default link tyupe, if specified
        """
        if self.default_link_name is not None:
            for t in self.template.templatetypes:
                if t.name.lower() == self.default_link_name.lower():
                    self.default_link_type = JSONObject({'id': t.id})

            if self.default_link_type is None:
                raise Exception(f"Default Link Type {self.default_link_name} "
                                f"not found in template {self.template.name}")

    def load_type_map(self):
        """
            Create the mapping from node to type
        """

        if self.type_csv is not None:
            self.load_type_map_from_csv()
        else:
            self.load_type_map_from_source_network()

    def load_type_map_from_csv(self):
        """
            Create a mapping from node name to type by getting the template
            and using a type_csv for the mapping
        """

        self.template = hb.db.DBSession.query(Template).filter(
            Template.id==self.template_id).first()

        if self.template is None:
            raise Exception(f"Template {self.template_id} not found")

        #map from type name to type object
        type_name_map = {}
        for tt in self.template.templatetypes:
            type_name_map[tt.name.lower()] = tt

        self.csv_df = pd.read_csv(self.type_csv, index_col='index')

        for nodename, typename in self.csv_df.itertuples():
            typename = typename.lower()
            if typename not in type_name_map:
                raise Exception(f"Type name {typename} not found in template {self.template_id}"
                                f"allowed typenames are: {type_name_map.keys()}")

            self.node_type_map[nodename] = type_name_map[typename]

    def get_network_type(self):
        """
            Look in the template for the template type for the NETWORK
        """
        if self.template_id is None:
            raise Exception("Can't get network type. Template is null")

        for tt in self.template.templatetypes:
            if tt.resource_type == 'NETWORK':
                return tt

        raise Exception(f"Unable to find NETWORK template type in template {self.template.id}")


    def load_type_map_from_source_network(self):
        """
            Create a mapping from node name to type by using a source network
        """
        self.source_network = hb.db.DBSession.query(Network).filter(
            Network.id == self.source_network_id).one()
        self.source_nodes = hb.db.DBSession.query(Node).filter(
            Node.network_id == self.source_network_id).all()
        self.source_links = hb.db.DBSession.query(Link).filter(
            Link.network_id == self.source_network_id).all()
        self.source_groups = hb.db.DBSession.query(ResourceGroup).filter(
            ResourceGroup.network_id == self.source_network_id).all()

        self.template = self.source_network.types[0].templatetype.template

        # SOURCE NODES
        for n in self.source_nodes:
            self.node_type_map[n.name] = n.types[0].templatetype
        # SOURCE LINKS
        for l in self.source_links:
            start_node = l.node_a.name
            end_node   = l.node_b.name
            self.link_type_map[(start_node, end_node)] = l.types[0].templatetype
        # SOURCE GROUPS
        for g in self.source_groups:
            self.group_type_map[g.name] = g.types[0].templatetype

    # generic function to add a resourcetype
    def add_resource_type(self, ref_key, target_resource, source_type):
        """
            Set the resource type of a resource. ref_key helps identify the type
            of resource, target_resource is the resource itself, and source type
            is the templatetype object to use
        """
        for t in target_resource.types:
            if t.type_id == source_type.id:
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

            rt.type_id = source_type.id
            rt.child_template_id = source_type.template_id
            self.new_resource_types[ref_key] = self.new_resource_types[ref_key] + 1
            hb.db.DBSession.add(rt)

    def check_networks_compatible(self):
        """
            Check the 2 networks are compatible by testing whether the source network
            contains all the nodes needed by the target network
        """
        #first check we've got compatible networks
        incompatible_nodes = []
        for n in self.target_nodes:
            if n.name not in self.node_type_map:
                incompatible_nodes.append(n.name)
        if len(incompatible_nodes) > 0:
            print(f"***ERROR*** Unable to map types. Found a node name(s) in"
                  f" target network {self.target_network_id} which is "
                  f"not in the source network {self.source_network_id} or type_csv.\n"
                  f"These are:  {incompatible_nodes}")
            return False

        #If a default link type is provided, it means that there are probably links the targtet
        #network not present in the source. If this is the case, then don't bother checking
        #for compatible links
        if self.default_link_type is None:
            for l in self.target_links:
                start_node = l.node_a.name
                end_node   = l.node_b.name
                if (start_node, end_node) not in self.link_type_map:
                    print(f"**ERROR** Unable to map types. Found a link name '{l.name}' in"
                          f" target network {self.target_network_id} which is "
                          f"not in the source network {self.source_network_id}")
                    return False

        for g in self.target_groups:
            if g.name not in self.group_type_map:
                print(f"Unable to map types. Found a group name '{g.name}' in"
                      f" target network {self.target_network_id} which is "
                      f"not in the source network {self.source_network_id}")
                return False
        return True
    def run(self):
        networks_compatible = self.check_networks_compatible()
        if (networks_compatible is False):
            return

        #OK we have compatible networks. Now create some resource types
        self.add_resource_type("NETWORK",
                                   self.target_network,
                                   self.get_network_type())

        for n in self.target_nodes:
            matching_type = self.node_type_map[n.name]
            self.add_resource_type("NODE", n, matching_type)


        for l in self.target_links:
            start_node = l.node_a.name
            end_node   = l.node_b.name
            matching_type = self.link_type_map.get((start_node, end_node), self.default_link_type)
            self.add_resource_type("LINK", l, matching_type)

        for g in self.target_groups:
            matching_type = self.group_type_map[g.name]
            self.add_resource_type("GROUP", g, matching_type)

        try:

            # To reenable later
            if self.debug is not True:
                hb.db.DBSession.flush()
                hb.commit_transaction()
                print("Inserted types: %s"%self.new_resource_types)
            else:
                print("**DEBUG MODE ENABLED** Inserted types: %s"%self.new_resource_types)
                print("**DEBUG MODE ENABLED** Transaction not commited to DB")
                hb.rollback_transaction()

        except Exception as e:
            print("An error has occurred: %s", e)
            hb.rollback_transaction()

@click.command()
@click.option('--source_network_id', type=int, default=None,
              help="(optional, see type_csv) The ID of the working network"+
              " i.e. the one where the nodes have correct types."+
              " This network must have the same node names as the broken network.")
@click.option('--type-csv', type=click.Path(), default=None,
              help="(optional, see source-network-id) A CSV file where the first column is the node names,"+
              "and the second column is the type names of that node."+
              "The columns must be headed 'index' and 'type'"+
              "*This must be used in conjunction with the --template-id argument")
@click.option('--template-id', type=int, default=None,
              help="(optional, see type-csv). This must be included if using the --type-csv"+
              "argument. This is the ID of the template to be applied to the target network")
@click.option('-t', '--target_network_id', type=int, default=None,
              help="The ID of the broken network i.e. the one where the nodes do not have types")
@click.option('--default-link', type=str, default=None,
              help="Name of a default link type, if a link exists in the target but not the source")
@click.option('--debug', is_flag=True, default=False, help="Don't commit the transaction if set.")
def restore(source_network_id=None, type_csv=None, template_id=None, target_network_id=None, default_link=None, debug=False):
    restorer = TypeRestorer(source_network_id, #either this OR
                            type_csv, #this and...
                            template_id,#...this
                            target_network_id, # mandatory
                            default_link=default_link,
                            debug=debug)
    restorer.load_data()
    restorer.run()

if __name__ == '__main__':
    restore()
