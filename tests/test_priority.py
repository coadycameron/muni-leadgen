from muni_leadgen.util import municipality_key, parse_priority, stable_bucket


def test_parse_priority_targets_middle_population():
    assert parse_priority(3276) == "Highest - Target"


def test_parse_priority_rejects_too_small():
    assert parse_priority(28) == "Low - Too Small"


def test_municipality_key():
    assert municipality_key("Orchard City", "Colorado") == "Orchard City|Colorado"


def test_stable_bucket():
    first = stable_bucket("Orchard City|Colorado")
    second = stable_bucket("Orchard City|Colorado")
    assert first == second
