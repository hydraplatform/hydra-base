from marshmallow import fields, ValidationError


class NodeField(fields.Field):
    """ Field to store a node_id

    This field serialises from/to a JSONObject representation of a Node
    """
    def _serialize(self, value, attr, obj):
        if value is not None:
            if isinstance(value, int):
                # Assume this is a node_id and store as is
                node_id = value
            else:
                # Assume we have JSON Object or Node instance
                node_id = value.id
            return node_id

    def _deserialize(self, value, attr, data):
        from hydra_base.lib.network import get_node
        return get_node(value)
