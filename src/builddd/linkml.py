from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from typing import Any
from enum import Enum

from builddd.dd import DataDictionary, DataType

import click
from linkml_runtime.dumpers import yaml_dumper
from linkml_runtime.linkml_model import SchemaDefinition, SlotDefinition
from linkml_runtime.utils.formatutils import camelcase, underscore
from linkml_runtime.utils.schemaview import SchemaView
from sqlalchemy import Column, ForeignKey, MetaData, Table, UniqueConstraint, create_mock_engine
from sqlalchemy.dialects.oracle import VARCHAR2
from sqlalchemy.types import Boolean, Date, DateTime, Enum, Float, Integer, Text, Time

from linkml._version import __version__
from linkml.transformers.relmodel_transformer import ForeignKeyPolicy, RelationalModelTransformer
from linkml.utils.generator import Generator, shared_arguments

__generator_version__ = "0.0.2"

logger = logging.getLogger(__name__)

import pdb


class SqlNamingPolicy(Enum):
    preserve = "preserve"
    underscore = "underscore"
    camelcase = "camelcase"


# TODO: move this up
METAMODEL_TYPE_TO_BASE = {
    "string": "str",
    "integer": "int",
    "boolean": "Bool",
    "float": "float",
    "double": "double",
    "decimal": "Decimal",
    "time": "XSDTime",
    "date": "XSDDate",
    "datetime": "XSDDateTime",
    "uriorcurie": "URIorCURIE",
    "uri": "URI",
    "ncname": "NCName",
    "objectidentifier": "ElementIdentifier",
    "nodeidentifier": "NodeIdentifier",
}

RANGEMAP = {
    "str": Text(),
    "string": Text(),
    "String": Text(),
    "NCName": Text(),
    "URIorCURIE": Text(),
    "int": Integer(),
    "Decimal": Integer(),
    "double": Float(),
    "float": Float(),
    "Bool": Boolean(),
    "URI": Text(),
    "XSDTime": Time(),
    "XSDDateTime": DateTime(),
    "XSDDate": Date(),
}

@dataclass
class LinkMLExtract(Generator):
    """
    A :class:`~linkml.utils.generator.Generator` for extracting CSV data dictionaries.

    The CSV export uses the schema production details described [here](https://linkml.io/linkml/generators/sqltable.html).

    The following changes are worth mentioning: 
    * Data Types are converted to match those described in the [Map Dragon DD Format](https://nih-ncpi.github.io/map-dragon/#/datadictionary).
    * Enumerations are extracted from the "permissible_values" field.
        * Descriptions are pulled from the description or title property, in that order
    * Units are identified by unit.ucum_code 

    The YAML file provided as a positional argument should be the top-level schema file that imports all other files. 

    """

    # ClassVars
    generatorname = os.path.basename(__file__)
    generatorversion = "0.1.0"
    valid_formats = ["sql"]                 # Required by generator.py
    uses_schemaloader = False               # Required for the MetaData function

    # ObjectVars
    use_foreign_keys: bool = True
    output_directory: str = "project/data-dictionary"

    def serialize(self, **kwargs: dict[str, Any]) -> str:
        return self.generate_ddl(**kwargs)

    def generate_ddl(self, naming_policy: SqlNamingPolicy = None, **kwargs: dict[str, Any]) -> str:
        """
        Generate a DDL using the schema in self.schema.

        :param naming_policy: naming policy for columns, defaults to None
        :type naming_policy: SqlNamingPolicy, optional
        :return: the DDL as a string
        :rtype: str
        """
        ddl_str = ""

        def dump(sql, *multiparams, **params):
            nonlocal ddl_str
            ddl_str += f"{str(sql.compile(dialect=engine.dialect)).rstrip()};"

        # pdb.set_trace()
        engine = create_mock_engine(f"{self.dialect}://./MyDb", strategy="mock", executor=dump)

        # We may not use this return, but it does some important stuff behind 
        # the scenes and, therefore, must be performed. 
        schema_metadata = MetaData()
        sqltr = RelationalModelTransformer(SchemaView(self.schema))
        if not self.use_foreign_keys:
            sqltr.foreign_key_policy = ForeignKeyPolicy.NO_FOREIGN_KEYS
        tr_result = sqltr.transform(tgt_schema_name=kwargs.get("tgt_schema_name"), top_class=kwargs.get("top_class"))
        schema = tr_result.schema

        def sql_name(n: str) -> str:
            if not naming_policy or naming_policy == SqlNamingPolicy.preserve:
                return n
            if naming_policy == SqlNamingPolicy.underscore:
                return underscore(n)
            if naming_policy == SqlNamingPolicy.camelcase:
                return camelcase(n)
            msg = f"Unknown: {naming_policy}"
            raise Exception(msg)

        sv = SchemaView(schema)

        dd = DataDictionary()
        # Iterate through the attributes in each class, creating Column objects.
        # This includes generating the appropriate column name, converting the range
        # into an SQL type, and adding a foreign key notation if appropriate.
        for cn, c in schema.classes.items():
            if not c['abstract']:
                tbl = dd.add_table(cn, c.description)

                pk_slot = sv.get_identifier_slot(cn)
                if c.attributes:
                    cols = []
                    for sn, s in c.attributes.items():
                        variable = tbl.add_variable(sn, s.description)
                        is_pk = "primary_key" in s.annotations
                        if pk_slot:
                            is_pk = sn == pk_slot.name

                        if is_pk:
                            variable.primary_key = True

                        variable.required = s.required
                        # else:
                        #    is_pk = True  ## TODO: use unique key
                        args = []
                        if s.range in schema.classes and self.use_foreign_keys:
                            fk = sql_name(self.get_id_or_key(s.range, sv))
                            args = [ForeignKey(fk)]
                            variable.comment = f"Foreign Key: {fk}"
                        field_type = self.get_sql_range(s, schema)
                        # print(type(field_type))
                        variable.set_type(field_type)
                        if variable.data_type == DataType.ENUM:
                            sv_enum = sv.get_enum(field_type.name)
                            for ename, enum in sv_enum['permissible_values'].items():
                                desc = enum['description']
                                if desc is None:
                                    desc = enum['title']
                                variable.add_enumeration(enum['text'], desc)

                            if len(variable.enumerations) == 0:
                                variable.comment = sv_enum['description'].strip()

                        if s.unit:
                            variable.units = f"UCUM:{s.unit['ucum_code']}"

        return dd.write_csv(self.output_directory)

    def get_sql_range(self, slot: SlotDefinition, schema: SchemaDefinition = None):
        """Get the slot range as a SQL Alchemy column type."""
        slot_range = slot.range

        if slot_range is None:
            return Text()

        # if no SchemaDefinition is explicitly provided as an argument
        # then simply use the schema that is provided to the LinkMLExtract() object
        if not schema:
            schema = self.schema

        sv = SchemaView(schema)
        if slot_range in sv.all_classes():
            # FK type should be the same as the identifier of the foreign key
            fk = sv.get_identifier_slot(slot_range)
            if fk:
                return self.get_sql_range(fk, sv.schema)
            return Text()

        if slot_range in sv.all_enums():
            e = sv.all_enums()[slot_range]
            if e.permissible_values is not None:
                vs = [str(v) for v in e.permissible_values]
                return Enum(name=e.name, *vs)

        if slot_range in METAMODEL_TYPE_TO_BASE:
            range_base = METAMODEL_TYPE_TO_BASE[slot_range]
        elif slot_range in sv.all_types():
            range_base = sv.all_types()[slot_range].base
        else:
            logger.error(f"Unknown range: {slot_range} for {slot.name} = {slot.range}")
            return Text()

        if range_base in RANGEMAP:
            return RANGEMAP[range_base]

        logger.error(f"Unknown range base: {range_base} for {slot.name} = {slot.range}")
        return Text()

    @staticmethod
    def get_id_or_key(cn: str, sv: SchemaView) -> str:
        """Given a named class, retrieve the identifier or key slot."""
        pk = sv.get_identifier_slot(cn, use_key=True)
        if pk is None:
            msg = f"No PK for {cn}"
            raise Exception(msg)
        pk_name = pk.alias if pk.alias else pk.name
        return f"{cn}.{pk_name}"


@shared_arguments(LinkMLExtract)
@click.command(name="csvdd")
@click.option(
    "--dialect",
    default="sqlite",
    show_default=True,
    help="SQL-Alchemy dialect, e.g. sqlite, mysql+odbc",
)
@click.option("--sqla-file", help="Path to sqlalchemy generated python")
@click.option(
    "--relmodel-output",
    help="Path to intermediate LinkML YAML of transformed relational model",
)
@click.option(
    "--output-directory",
    default="project/data-dictionary",
    show_default=True,
    help="Specify where data-dictionary files are to be written."
)
@click.version_option(__generator_version__, "-V", "--version")
def cli(
    yamlfile: str,
    relmodel_output: str,
    sqla_file: str | None = None,
    dialect: str | None = None,
    use_foreign_keys: bool = True,
    **args,
):
    gen = LinkMLExtract(yamlfile, use_foreign_keys=use_foreign_keys, **args)
    if dialect:
        gen.dialect = dialect

    print(gen.generate_ddl())

if __name__ == "__main__":
    cli()
