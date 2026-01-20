"""
АГРЕССИВНАЯ ОЧИСТКА REDIS
Этот скрипт удаляет ВСЕ данные Celery из Redis
"""

import redis
import sys

def aggressive_cleanup():
    """Полная очистка всех ключей Celery"""
    try:
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=False)
        r.ping()
        print("✅ Подключение к Redis успешно\n")
        
        # Паттерны для поиска всех ключей Celery
        patterns = [
            b'celery-task-meta-*',
            b'_kombu.*',
            b'unacked*',
            b'unacked_index',
            b'celery',
            b'*celery*',
        ]
        
        total_deleted = 0
        
        print("🔍 Поиск и удаление ключей Celery...\n")
        
        for pattern in patterns:
            try:
                keys = r.keys(pattern)
                if keys:
                    print(f"   Найдено {len(keys)} ключей по шаблону: {pattern.decode('utf-8', errors='ignore')}")
                    for key in keys:
                        try:
                            r.delete(key)
                            total_deleted += 1
                        except:
                            pass
            except Exception as e:
                print(f"   ⚠️  Ошибка при обработке pattern {pattern}: {e}")
        
        # Дополнительно: удаляем все ключи с битыми данными
        print("\n🔍 Поиск битых данных...")
        all_keys = r.keys(b'*')
        broken_keys = 0
        
        for key in all_keys:
            try:
                # Пытаемся получить тип ключа
                key_type = r.type(key)
                
                # Если это строка, пытаемся прочитать
                if key_type == b'string':
                    try:
                        value = r.get(key)
                        # Проверяем на битые Celery данные
                        if value and (b'exc_type' in value or b'celery' in value.lower()):
                            r.delete(key)
                            broken_keys += 1
                    except:
                        # Если не можем прочитать - удаляем
                        r.delete(key)
                        broken_keys += 1
            except:
                # Любая ошибка - удаляем ключ
                try:
                    r.delete(key)
                    broken_keys += 1
                except:
                    pass
        
        if broken_keys > 0:
            print(f"   Удалено {broken_keys} битых ключей")
            total_deleted += broken_keys
        
        print(f"\n✅ ОЧИСТКА ЗАВЕРШЕНА!")
        print(f"🗑️  Всего удалено ключей: {total_deleted}")
        
        # Проверяем что осталось
        remaining = r.dbsize()
        print(f"📊 Ключей осталось в базе: {remaining}")
        
        if remaining > 0:
            print("\n⚠️  В базе остались ключи. Показываю первые 10:")
            sample_keys = r.keys(b'*')[:10]
            for key in sample_keys:
                key_str = key.decode('utf-8', errors='ignore')
                print(f"   - {key_str}")
            
            print("\n❓ Удалить ВСЕ оставшиеся ключи? (да/нет): ", end='')
            response = input().lower().strip()
            
            if response in ['да', 'yes', 'y', 'д']:
                r.flushdb()
                print("✅ База полностью очищена!")
        
        print("\n" + "="*60)
        print("СЛЕДУЮЩИЕ ШАГИ:")
        print("="*60)
        print("1. Перезапустите Celery worker:")
        print("   celery -A app.tasks.tasks_parsing worker --loglevel=info --concurrency=4")
        print("\n2. Попробуйте загрузить файл снова")
        print("\n3. Если ошибка повторится - сообщите мне")
        print("="*60)
        
    except redis.ConnectionError:
        print("❌ ОШИБКА: Не удалось подключиться к Redis!")
        print("   Убедитесь что Redis запущен на localhost:6379")
        print("\n   Проверьте:")
        print("   1. Redis Server запущен")
        print("   2. Порт 6379 открыт")
        print("   3. Нет проблем с подключением")
        sys.exit(1)
        
    except Exception as e:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def nuclear_option():
    """Ядерная опция - удаляет ВСЁ из Redis"""
    try:
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.ping()
        
        keys_before = r.dbsize()
        print(f"\n📊 В базе {keys_before} ключей")
        print("\n⚠️⚠️⚠️  ВНИМАНИЕ ⚠️⚠️⚠️")
        print("Это удалит АБСОЛЮТНО ВСЕ данные из Redis базы данных 0")
        print("Включая:")
        print("  - Все результаты задач Celery")
        print("  - Все очереди")
        print("  - ВСЕ другие данные в этой базе")
        print("\nЭто необратимая операция!")
        
        print("\n❓ Вы УВЕРЕНЫ? Напишите 'УДАЛИТЬ ВСЁ' для подтверждения: ", end='')
        response = input().strip()
        
        if response == 'УДАЛИТЬ ВСЁ':
            print("\n🗑️  Удаление всех данных...")
            r.flushdb()
            print("✅ База полностью очищена!")
            print(f"🗑️  Удалено ключей: {keys_before}")
            
            print("\n" + "="*60)
            print("БАЗА REDIS ПОЛНОСТЬЮ ОЧИЩЕНА")
            print("="*60)
            print("Теперь перезапустите Celery worker")
            print("="*60)
        else:
            print("❌ Операция отменена (неверный ввод)")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    print("="*60)
    print("ОЧИСТКА REDIS ОТ ПРОБЛЕМНЫХ ДАННЫХ CELERY")
    print("="*60)
    print()
    print("У вас проблема с сериализацией данных в Redis.")
    print("Нужно удалить старые данные чтобы Celery работал корректно.")
    print()
    print("Выберите режим:")
    print()
    print("1. Агрессивная очистка (удаляет все ключи Celery + битые данные)")
    print("2. Ядерная опция (УДАЛЯЕТ ВСЁ из Redis базы 0)")
    print("3. Отмена")
    print()
    
    choice = input("Ваш выбор (1/2/3): ").strip()
    print()
    
    if choice == '1':
        print("="*60)
        aggressive_cleanup()
    elif choice == '2':
        nuclear_option()
    else:
        print("❌ Операция отменена")