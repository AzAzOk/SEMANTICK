from app.core.chanking import TextSplitter, DocumentChunker, BusinessMetadata
from app.core.parsers_system import ParserManager
from app.database import client, add_chunks_to_qdrant, reserch_similar_chunks





def main():
    path = "C:\\Users\\kulikovMA\\Desktop\\task_1\\12.dxf"
    manager = ParserManager()
    ext = manager._parser_extension(path)
    # print(ext)
    find = manager._find_parser_in_registry(ext)
    # print(find)
    result_parser = manager._ransfer_selected_parser(path, find)
    # print(result_parser)
    # print("ssssssssssssssss")
    splitter = TextSplitter()
    chancers = splitter.split_text(result_parser.text)
    # print(chancers)
    data_meatadata = BusinessMetadata()
    metaDocument = DocumentChunker(chancers)
    # result_uniter = metaDocument.uniter(result_parser.metadata, path, manager._file_name(path), ext, data_meatadata)
    # print(result_uniter)
    # points = add_chunks_to_qdrant(client, result_uniter)
    # print(points)
    # reserch = reserch_similar_chunks(client, 
    #     "структуры объекта автоматизации особенности его функционирования"
    # )
    # print('\n \n \n \n ')
    # print(reserch)
