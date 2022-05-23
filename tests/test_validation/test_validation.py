import pytest
from actions.validation.event import validation


@pytest.fixture()
def start_main_class():
    start = validation.Validation()
    return start


def test_general(start_main_class):
    print(start_main_class.handler())


def test_get_migrated_schemas(start_main_class):
    assert type(start_main_class.get_migrated_schemas()) == list


def test_get_generic_properties(start_main_class):
    assert start_main_class.get_generic_properties(
        start_main_class.get_migrated_schemas()[0][1]
    )


def test_get_migrated_schema_properties(start_main_class):
    assert start_main_class.get_migrated_schema_properties(
        start_main_class.get_migrated_schemas()[0][0]
    )


def test_clean_names(start_main_class):
    cleaned_names = list(
        map(
            start_main_class.clean_name_properties,
            [i[1] for i in start_main_class.get_migrated_schema_properties(
                start_main_class.get_migrated_schemas()[0][0])
             ],
        )
    )
    assert cleaned_names


def test_get_event_names(start_main_class):
    assert start_main_class.get_event_names(
        cleaned_names=list(
            map(
                start_main_class.clean_name_properties,
                [i[1] for i in start_main_class.get_migrated_schema_properties(
                    start_main_class.get_migrated_schemas()[0][0])
                 ],
            )
        ),
        generic_properties=start_main_class.get_generic_properties(
            start_main_class.get_migrated_schemas()[0][1]
        )
    )


def test_get_user_events_properties(start_main_class):
    assert type(start_main_class.get_user_events_properties(
        event_name=start_main_class.get_event_names(
            cleaned_names=list(
                map(
                    start_main_class.clean_name_properties,
                    [i[1] for i in start_main_class.get_migrated_schema_properties(
                        start_main_class.get_migrated_schemas()[0][0])
                     ],
                )
            ),
            generic_properties=start_main_class.get_generic_properties(
                start_main_class.get_migrated_schemas()[0][1]
            )
        )[0]
    )) == list