def evaluate_condition(pub_val, op, sub_val):
    if op == "=":
        return pub_val == sub_val
    elif op == "!=":
        return pub_val != sub_val
    elif op == ">":
        return pub_val > sub_val
    elif op == "<":
        return pub_val < sub_val
    elif op == ">=":
        return pub_val >= sub_val
    elif op == "<=":
        return pub_val <= sub_val
    return False

def matches(publication, subscription):
    """
    Checks if a publication matches a subscription.
    publication: messages_pb2.Publication
    subscription: messages_pb2.Subscription
    """
    for cond in subscription.conditions:
        field = cond.field
        
        # Check if field exists in publication
        if cond.is_string:
            if field not in publication.string_fields:
                return False
            pub_val = publication.string_fields[field]
            sub_val = cond.string_value
        else:
            if field not in publication.double_fields:
                return False
            pub_val = publication.double_fields[field]
            sub_val = cond.double_value
            
        if not evaluate_condition(pub_val, cond.operator, sub_val):
            return False
            
    return True
