-- This macro does not exist, but should be caught be the jinja handler and warned about.
SELECT {{ non_existant_macro() }}