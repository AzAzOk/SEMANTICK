import wx
from app.database import client, add_chunks_to_qdrant, reserch_similar_chunks
# from main_gui import text_print, text_field

class Window(wx.Frame):

    def __init__(self, parent, title):
        wx.Frame.__init__(self, parent, title = title, size = (300,500))
        self.Maximize(True)
        self.panel = wx.Panel(self)

        self.text_field_i = self.text_input(
        pos=(960, 1100), 
        size=(700, 40),
        value_gost="Введите сообщение"
    )
        self.output_field = self.text_print(
                pos=(560, 50), 
                size=(1500, 1000),
                # value_gost="Введите сообщение"
            )

        # Связываем событие нажатия Enter для поля text_field_i и output_field
        self.text_ctrl.Bind(
            wx.EVT_TEXT_ENTER,
            lambda event: self.on_enter_pressed(
                event, input_ctrl=self.text_field_i, output_ctrl=self.output_field
            ),
        )

        self.Show(True)


    def text_input(self, pos, size, value_gost):

        """Поле ввода текста"""

        self.text_ctrl = wx.TextCtrl(
            self.panel,
            pos=pos,
            size=size,
            style=wx.TE_PROCESS_ENTER
        )
        self.text_ctrl.SetHint(value_gost)
        return self.text_ctrl
    

    def on_enter_pressed(self, event, input_ctrl, output_ctrl):

        """"Обработчик нажатия Enter в поле ввода текста"""

        # сохраняем введённый текст сразу
        text = self.get_text(input_ctrl)
        if not text or not text.strip():
            # ничего не делать для пустого ввода
            event.Skip()
            return

        current_text = self.get_text(output_ctrl)
        new_text = f"{current_text}\n>     {text}" if current_text else f">     {text}"
        self.set_text(new_text, output_ctrl)

        try:
            # Передаём сохранённый текст
            search_result = reserch_similar_chunks(client, text)
            print("\n".join([result['text'] for result in search_result]))

            # Форматируем результат для вывода (append, чтобы не перезаписывать лог)
            if search_result:
                output_ctrl.AppendText("\n" + "="*80)
                
                for i, result in enumerate(search_result, 1):
                    score_percent = result['score'] * 100
                    metadata = result['metadata']
                    
                    output_ctrl.AppendText(f"\n\n📄 РЕЗУЛЬТАТ #{i}")
                    output_ctrl.AppendText(f"\n{'─'*40}")
                    output_ctrl.AppendText(f"\n🆔 ID: {result['id']}")
                    output_ctrl.AppendText(f"\n🎯 СХОДСТВО: {score_percent:.1f}%")

                    output_ctrl.AppendText(f"\n   📂 Файл: {metadata.get('file_name', 'Неизвестно')}")
                    output_ctrl.AppendText(f"\n   📍 Путь: {metadata.get('file_path', 'Неизвестно')}")
                    output_ctrl.AppendText(f"\n   🔤 Расширение: {metadata.get('file_extension', 'Неизвестно')}")
                    
                    output_ctrl.AppendText(f"\n\n📝 ТЕКСТ:")
                    output_ctrl.AppendText(f"\n{result['text']}")
                    output_ctrl.AppendText(f"\n{'─'*80}")
                    
            else:
                output_ctrl.AppendText("\n❌ По вашему запросу ничего не найдено")

        except Exception as exc:
            # Показываем ошибку в поле вывода — не падаем
            import traceback
            tb = traceback.format_exc()
            self.set_text(f"Ошибка при поиске:\n{exc}\n\n{tb}", output_ctrl)

        # очищаем поле ввода только в конце
        self.set_text("", input_ctrl)
        event.Skip()
    
    
    def text_print(self, pos, size, value_gost = None):

        """Поле вывода текста"""

        self.output_field = wx.TextCtrl(
            self.panel,
            # value="Результаты будут отображены здесь...\n",
            pos=pos,
            size=size,
            style=wx.TE_READONLY | wx.TE_MULTILINE | wx.TE_RICH2
        )
        return self.output_field
    
    
    def get_text(self, input_ctrl) -> str:

        """Получает текст из поля вывода текста"""

        return input_ctrl.GetValue()
    

    def set_text(self, text: str, output_ctrl):

        """Устанавливает текст в поле вывода текста"""

        output_ctrl.SetValue(text)