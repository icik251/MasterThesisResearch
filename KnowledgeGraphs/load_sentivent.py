list_of_dicts_type = []
list_of_dicts_subtype = []
with open(
    "D:\PythonProjects\MasterThesisResearch\KnowledgeGraphs\data\sentivent\processed\dataset_event_type.tsv",
    encoding="utf-8",
) as ftype, open(
    "D:\PythonProjects\MasterThesisResearch\KnowledgeGraphs\data\sentivent\processed\dataset_event_subtype.tsv",
    encoding="utf-8",
) as fsubtype:
    count = 0
    for line_type, line_subtype in zip(ftype.readlines(), fsubtype.readlines()):
        if count == 0:
            list_of_columns_type = line_type.split("\t")
            list_of_columns_subtype = line_subtype.split("\t")
            count+=1
        else:
            list_of_dicts_type.append(dict(zip(list_of_columns_type, line_type.split("\t"))))
            list_of_dicts_subtype.append(dict(zip(list_of_columns_subtype, line_subtype.split("\t"))))
