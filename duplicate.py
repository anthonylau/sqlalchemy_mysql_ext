#! /usr/bin/env python

# Copyright 2010 Brian Edwards. All Rights Reserved.

from sqlalchemy import exc
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert, TableClause

class InsertOnDuplicate(Insert):
    pass

def insertOnDuplicate(table, values=None, inline=False, **kwargs):
    return InsertOnDuplicate(table, values, inline=inline, **kwargs)

TableClause.insertOnDuplicate = insertOnDuplicate

@compiles(InsertOnDuplicate)
def compile_insertOnDuplicateKeyUpdate(insert_stmt, compiler, **kw):
    compiler.isinsert = True
    colparams = compiler._get_colparams(insert_stmt)
    if not colparams and \
            not compiler.dialect.supports_default_values and \
            not compiler.dialect.supports_empty_insert:
        raise exc.CompileError("The version of %s you are using does "
                                "not support empty inserts." % 
                                compiler.dialect.name)
    preparer = compiler.preparer
    supports_default_values = compiler.dialect.supports_default_values
    text = "INSERT"
    prefixes = [compiler.process(x) for x in insert_stmt._prefixes]
    if prefixes:
        text += " " + " ".join(prefixes)
    text += " INTO " + preparer.format_table(insert_stmt.table)
    if colparams or not supports_default_values:
        text += " (%s)" % ', '.join([preparer.format_column(c[0])
                   for c in colparams])
    if compiler.returning or insert_stmt._returning:
        compiler.returning = compiler.returning or insert_stmt._returning
        returning_clause = compiler.returning_clause(insert_stmt, compiler.returning)
        if compiler.returning_precedes_values:
            text += " " + returning_clause
    if not colparams and supports_default_values:
        text += " DEFAULT VALUES"
    else:
        text += " VALUES (%s)" % \
                 ', '.join([c[1] for c in colparams])
    if compiler.returning and not compiler.returning_precedes_values:
        text += " " + returning_clause
    text += ' ON DUPLICATE KEY UPDATE ' + \
            ', '.join(
                    compiler.preparer.quote(c[0].name, c[0].quote) + '=' + c[1]
                  for c in colparams
            )
    return text
