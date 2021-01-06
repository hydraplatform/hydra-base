"""
This is a utility which takes a source network ID and target network ID,
and sets the types from the source onto the target -- this is needed when
the resource types for the target network don't exist.

The names of the nodes in both networks need to be the same
"""
import hydra_base as hb
from hydra_base.db.model import *


def run(source_network_id, target_network_id):
    hb.db.connect()

    source_nodes = hb.db.DBSession.query(Node).filter(Node.network_id == source_network_id).all()
    source_links = hb.db.DBSession.query(Link).filter(Link.network_id == source_network_id).all()
    source_groups = hb.db.DBSession.query(ResourceGroup).filter(ResourceGroup.network_id == source_network_id).all()
    #map node name to type name
    node_type_map = {}
    link_type_map = {}
    group_type_map = {}
    for n in source_nodes:
        node_type_map[n.name] = n.types[0]
    for l in source_links:
        start_node = l.node_a.name
        end_node   = l.node_b.name
        link_type_map[(start_node, end_node)] = l.types[0]
    for g in source_groups:
        group_type_map[g.name] = g.types[0]

    new_resource_types = []
    target_nodes = hb.db.DBSession.query(Node).filter(Node.network_id == target_network_id).all()
    target_links = hb.db.DBSession.query(Link).filter(Link.network_id == target_network_id).all()
    target_groups = hb.db.DBSession.query(ResourceGroup).filter(ResourceGroup.network_id == target_network_id).all()

    #first check we've got compatible networks
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
    for n in target_nodes:
        matching_type = node_type_map[n.name]

        for t in n.types:
            if t.type_id == matching_type.type_id:
                print(f"Node {n.name} already has the correct type")
                continue

        new_resource_types.append(
                    {
                        'ref_key' : 'NODE',
                        'node_id' : n.id,
                        'type_id' : matching_type.type_id,
                        'child_template_id' : matching_type.child_template_id
                    })
    for l in target_links:
        start_node = l.node_a.name
        end_node   = l.node_b.name
        matching_type = link_type_map[(start_node, end_node)]
        new_resource_types.append(
                    {
                        'ref_key' : 'LINK',
                        'link_id' : l.id,
                        'type_id' : matching_type.type_id,
                        'child_template_id' : matching_type.child_template_id
                    })
    for g in target_groups:
        matching_type = group_type_map[g.name]
        new_resource_types.append(
                    {
                        'ref_key' : 'GROUP',
                        'group_id' : g.id,
                        'type_id' : matching_type.type_id,
                        'child_template_id' : matching_type.child_template_id
                    })

    try:

        if len(new_resource_types) > 0:
            print("Inserting %s types"%len(new_resource_types))
            hb.db.DBSession.bulk_insert_mappings(ResourceType, new_resource_types)

        hb.db.DBSession.flush()

        hb.db.commit_transaction()
        print("Inserted: %s types"%len(new_resource_types))
    except Exception as e:
        print("An error has occurred: %s", e)
        hb.db.rollback_transaction()

if __name__ == '__main__':
    network_with_types = 4549
    network_without_types = 4538
    run(network_with_types,network_without_types)

