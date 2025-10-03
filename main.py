from app.core.chanking import TextSplitter, DocumentChunker
from app.core.parsers_system import ParserManager




if __name__ == "__main__":
    path = "C:\\Users\\kulikovMA\\Desktop\\Statia.docx"
    manager = ParserManager()
    ext = manager._parser_extension(path)
    print(ext)
    find = manager._find_parser_in_registry(ext)
    print(find)
    result_parser = manager._ransfer_selected_parser(path, find)
    print(result_parser)
    splitter = TextSplitter()
    chancers = splitter.split_text(result_parser.text)
    print(chancers)

    metaDocument = DocumentChunker(chancers)
    metaDocument.uniter()
    print(metaDocument.unit_chank)
