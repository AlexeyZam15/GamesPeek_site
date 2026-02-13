// games/static/games/js/game_list/ajax-pagination-integration.js
/**
 * Интеграция AJAX-пагинации с фильтрами
 * Добавляет в FilterManager методы для работы с пагинацией
 */

(function() {
    // Сохраняем ссылку на оригинальный метод submitForm
    const originalSubmitForm = window.FilterManager?.submitForm;

    if (window.FilterManager) {
        console.log('AjaxPagination: Integrating with FilterManager...');

        // Переопределяем submitForm для сброса пагинации при применении фильтров
        window.FilterManager.submitForm = function(e) {
            // Вызываем оригинальный метод
            if (originalSubmitForm) {
                // Сохраняем результат
                const result = originalSubmitForm.apply(this, arguments);

                // Сбрасываем пагинацию на первую страницу
                if (window.AjaxPagination && window.AjaxPagination.state?.isInitialized) {
                    console.log('AjaxPagination: Filters applied, resetting to page 1');

                    // Добавляем задержку для завершения сабмита формы
                    setTimeout(() => {
                        // Если форма отправляется через GET (стандартный редирект)
                        // то нам не нужно ничего делать - страница перезагрузится

                        // Если же используется AJAX для фильтров, можно добавить логику
                        if (window.FilterManager?.useAjax) {
                            window.AjaxPagination.reset();
                        }
                    }, 100);
                }

                return result;
            }
        };

        console.log('AjapPagination: FilterManager integration complete');
    }

    // Экспорт для использования в других модулях
    if (typeof module !== 'undefined' && module.exports) {
        module.exports = { integrateWithFilters: true };
    }
})();