from django import forms

class UploadFileForm(forms.Form):
    file = forms.FileField()

class Analysis(forms.Form):
    # Поля для выбора временного периода
    start_date = forms.DateField(required=False, label="Начиная с", widget=forms.DateInput(attrs={'type': 'date'}))
    end_date = forms.DateField(required=False, label="Заканчивая", widget=forms.DateInput(attrs={'type': 'date'}))

    # Поле для выбора параметра
    PARAMETER_CHOICES = [
        ('opt1', 'Распределение  запросов по типу'),
        ('opt2', '10 наиболее активных API'),
        ('opt3', 'Среднее время ответа сервера'),
        ('opt4', 'Рефереры с наибольшим количеством трафика'),
        ('opt5', 'Распределение запросов по количеству дней недели'),
        ('opt6', 'Наиболее требовательные запросы'),
        ('opt7', 'API с самым высоким уровнем ответа'),
    ]
    parameter = forms.ChoiceField(choices=PARAMETER_CHOICES, required=False, label="Анализируемая статистика")
