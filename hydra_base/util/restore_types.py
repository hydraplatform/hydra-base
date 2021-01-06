"""
This is a utility which takes a source network ID and target network ID,
and sets the types from the source onto the target -- this is needed when
the resource types for the target network don't exist.

The names of the nodes in both networks need to be the same
"""
import hydra_base as hb
from hydra_base.db.model import *

# generic function to add a resourcetype
def tryAddingResourceType(ref_key, target_resource, source_resource_type):
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
        new_resource_types.append(rt)
        hb.db.DBSession.add(rt)
        counters[rt.ref_key] = counters[rt.ref_key] + 1



def run(source_network_id, target_network_id):
    hb.db.connect()

    source_networks = hb.db.DBSession.query(Network).filter(Network.id == source_network_id).all()
    source_nodes = hb.db.DBSession.query(Node).filter(Node.network_id == source_network_id).all()
    source_links = hb.db.DBSession.query(Link).filter(Link.network_id == source_network_id).all()
    source_groups = hb.db.DBSession.query(ResourceGroup).filter(ResourceGroup.network_id == source_network_id).all()
    #map node name to type name
    network_type_map = {}
    node_type_map = {}
    link_type_map = {}
    group_type_map = {}
    # SOURCE NETWORK
    for n in source_networks:
        network_type_map[n.name] = n.types[0]
    # SOURCE NODES
    for n in source_nodes:
        node_type_map[n.name] = n.types[0]
    # SOURCE LINKS
    for l in source_links:
        start_node = l.node_a.name
        end_node   = l.node_b.name
        link_type_map[(start_node, end_node)] = l.types[0]
    # SOURCE GROUPS
    for g in source_groups:
        group_type_map[g.name] = g.types[0]

    new_resource_types = []
    target_networks = hb.db.DBSession.query(Network).filter(Network.id == target_network_id).all()
    target_nodes = hb.db.DBSession.query(Node).filter(Node.network_id == target_network_id).all()
    target_links = hb.db.DBSession.query(Link).filter(Link.network_id == target_network_id).all()
    target_groups = hb.db.DBSession.query(ResourceGroup).filter(ResourceGroup.network_id == target_network_id).all()

    counters={'NODE': 0, 'NETWORK': 0, 'LINK': 0, 'GROUP': 0}

    #first check we've got compatible networks
    for n in target_networks:
        if n.name not in network_type_map:
            raise Exception(f"Unable to map types. The network type for the network name '{n.name}' is"
                            f" different between the target network id {target_network_id} and the  "
                            f" source network {source_network_id}")

    for n in target_nodes:
        if n.name not in node_type_map:
            raise Exception(f"Unable to map types. Found a node name '{n.name}' in"
                            f" target network {target_network_id} which is "
                            f"not in the source network {source_network_id}")

    for l in target_links:
        start_node = l.node_a.name
        end_node   = l.node_b.name
        if (start_node, end_node) not in link_type_map:
            raise Exception(f"Unable to map types. Found a link name '{l.name}' in"
                            f" target network {target_network_id} which is "
                            f"not in the source network {source_network_id}")

    #OK we have compatible networks. Now create some resource types
    for n in target_networks:
        matching_type = network_type_map[n.name]

        # Please check
        tryAddingResourceType("NETWORK", n, matching_type)


        # for t in n.types:
        #     if t.type_id == matching_type.type_id:
        #         print(f"Network {n.name} already has the correct type")
        #         break
        # else:
        #     print(f"Adding the correct type for the Network {n.name}")
        #     rt = ResourceType()
        #     rt.ref_key = 'NETWORK'
        #     rt.network_id = n.id
        #     rt.type_id = matching_type.type_id
        #     rt.child_template_id = matching_type.child_template_id
        #     hb.db.DBSession.add(rt)
        #     new_resource_types.append(rt)
        #     counters[rt.ref_key] = counters[rt.ref_key] + 1


    for n in target_nodes:
        matching_type = node_type_map[n.name]

        # Please check
        tryAddingResourceType("NODE", n, matching_type)

        # for t in n.types:
        #     if t.type_id == matching_type.type_id:
        #         print(f"Node {n.name} already has the correct type")
        #         break
        # else:
        #     rt = ResourceType()
        #     rt.ref_key = 'NODE'
        #     rt.node_id = n.id
        #     rt.type_id = matching_type.type_id
        #     rt.child_template_id = matching_type.child_template_id
        #     hb.db.DBSession.add(rt)
        #     new_resource_types.append(rt)
        #     counters[rt.ref_key] = counters[rt.ref_key] + 1

    for l in target_links:
        start_node = l.node_a.name
        end_node   = l.node_b.name
        matching_type = link_type_map[(start_node, end_node)]

        # Please check
        tryAddingResourceType("LINK", l, matching_type)

        # for t in l.types:
        #     if t.type_id == matching_type.type_id:
        #         print(f"Link {l.name} already has the correct type")
        #         break
        # else:
        #     rt = ResourceType()
        #     rt.ref_key = 'LINK'
        #     rt.link_id = l.id
        #     rt.type_id = matching_type.type_id
        #     rt.child_template_id = matching_type.child_template_id
        #     hb.db.DBSession.add(rt)
        #     new_resource_types.append(rt)
        #     counters[rt.ref_key] = counters[rt.ref_key] + 1

    for g in target_groups:
        matching_type = group_type_map[g.name]

        # Please check
        tryAddingResourceType("GROUP", g, matching_type)

        # for t in g.types:
        #     if t.type_id == matching_type.type_id:
        #         print(f"Group {g.name} already has the correct type")
        #         break
        # else:
        #     rt = ResourceType()
        #     rt.ref_key = 'GROUP'
        #     rt.group_id = g.id
        #     rt.type_id = matching_type.type_id
        #     rt.child_template_id = matching_type.child_template_id
        #     new_resource_types.append(rt)
        #     hb.db.DBSession.add(rt)
        #     counters[rt.ref_key] = counters[rt.ref_key] + 1


    try:

#        if len(new_resource_types) > 0:
#            print("Inserting %s types"%len(new_resource_types))
#            hb.db.DBSession.bulk_insert_mappings(ResourceType, new_resource_types)


        # To reenable later
        # hb.db.DBSession.flush()
        # hb.commit_transaction()

        hb.rollback_transaction()

#        print("Inserted: %s types"%len(new_resource_types))
        print("Inserted: %s types"%len(new_resource_types))
        print(counters)
    except Exception as e:
        print("An error has occurred: %s", e)
        hb.rollback_transaction()

if __name__ == '__main__':
    network_with_types = 4549
    network_without_types = 4538
    run(network_with_types,network_without_types)
