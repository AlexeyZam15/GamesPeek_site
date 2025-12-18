from django.db.models.signals import m2m_changed
from django.dispatch import receiver


@receiver(m2m_changed, sender=Game.keywords.through)
def update_keyword_count(sender, instance, action, **kwargs):
    """
    Автоматически обновляет счетчик использования при изменении связи
    """
    if action in ['post_add', 'post_remove', 'post_clear']:
        # Получаем все затронутые ключевые слова
        if action == 'post_clear':
            # При полной очистке получаем старые ключевые слова
            if hasattr(instance, '_prefetched_objects_cache'):
                keywords = instance._prefetched_objects_cache.get('keywords', [])
            else:
                keywords = list(instance.keywords.all())
        else:
            # При добавлении/удалении получаем PK из kwargs
            keyword_ids = kwargs.get('pk_set', set())
            keywords = Keyword.objects.filter(id__in=keyword_ids)

        # Обновляем счетчики для каждого ключевого слова
        for keyword in keywords:
            keyword.update_cached_count()