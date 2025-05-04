# Библиотека Shapes

Простая библиотека на Python для создания, расширения и регистрации классов геометрических фигур.

## Требования

- Python 3.11.5 или новее

## Установка

1. Установите зависимости (необходимы, например, для запуска тестов):

```bash
pip install -r requirements.txt
```

> Содержимое `requirements.txt`:
> ```
> pytest
> ```

## Пример использования


### Использование встроенных фигур

```python
from shapes.shapes import create_shape, list_shapes

print(list_shapes())  # доступные для создания фигуры

circle = create_shape("circle", radius=5) # создать круг
circle.area()                             # вычислить площадь

triangle = create_shape("triangle", a=3, b=4, c=5)
triangle.area()
```

### Является ли треугольник прямоугольным

```python
from shapes.shapes import create_shape
from shapes.geometry.utils import is_right_angled   # импорт необходимой функции

triangle = create_shape("triangle", a=3, b=4, c=5)
is_right_angled(triangle)                           
```

### Пользовательская фигура

Ниже приведён минимальный пример того, как определить пользовательскую фигуру и зарегистрировать её:

```python
class Rectangle(Shape):
    def __init__(self, a, b, c, d):
        self.a = a
        self.b = b
        self.c = c
        self.d = d 
    
    # переопределить метод _compute_area
    def _compute_area(self):
        return self.a * self.b 

    # переопределить метод _is_valid
    def _is_valid(self):
        return all(side > 0 for side in (self.a, self.b, self.c, self.d))
        
register_shape("rectangle", Rectangle)

rect = Rectangle(4, 5, 6, 3)
rect.area()
```

## Тесты

В проекте используется `pytest` для автоматического тестирования. Тесты находятся в папке `shapes/tests/`.

Пример теста пользовательского сценария:

```python
def test_user_defined_shape():

    class Rectangle(Shape):
        def __init__(self, a, b, c, d):
            self.a = a
            self.b = b
            self.c = c
            self.d = d 
            
        def _compute_area(self):
            return self.a * self.b 
        
        def _is_valid(self):
            return all(side > 0 for side in (self.a, self.b, self.c, self.d))
        
    register_shape("rectangle", Rectangle)
    
    assert "rectangle" in list_shapes()
    rect = Rectangle(4, 5, 6, 3)
    assert rect.area() == 20
```

Этот тест проверяет, что:

- Класс, определённый пользователем, успешно регистрируется.
- Класс корректно наследует `Shape`.
- Метод `area()` работает как ожидается.

## Запуск тестов

Для запуска всех тестов убедитесь, что вы находитесь в корне проекта, и выполните:

```bash
pytest
```

## Лицензия

MIT License