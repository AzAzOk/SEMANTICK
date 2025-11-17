import wx
from .window_main import Window
from app.core.chanking import TextSplitter, DocumentChunker, BusinessMetadata
from app.core.parsers_system import ParserManager
from app.database import client, add_chunks_to_qdrant, reserch_similar_chunks
from main import main
# main()
app = wx.App()
wnd = Window(None, 'Семантический поиск')

# Создаем кнопку для демонстрации работы с текстом
# button = wx.Button(wnd.panel, label="Получить текст", pos=(50, 100))

# def on_button_click(event):
#     text = wnd.get_text()
#     print(f"Текст из поля: {text}")
#     # Меняем текст
#     wnd.set_text(f"Изменено: {text}")

# button.Bind(wx.EVT_BUTTON, on_button_click)
# text = wnd.get_text(wnd.text_field_i)
# wnd.set_text(text=text, output_ctrl=wnd.output_field)
# reserch = reserch_similar_chunks(client, wnd.get_text(wnd.text_field_i))
# print('\n \n \n \n ')
# wnd.set_text(reserch, wnd.output_field)

wnd.Show(True)
app.MainLoop()