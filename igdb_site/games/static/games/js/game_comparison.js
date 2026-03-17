// games/static/games/js/game_comparison.js

/**
 * Инициализация тултипов Bootstrap
 */
function initTooltips() {
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
}

/**
 * Подсветка хедера в зависимости от процента схожести
 * @param {number} similarityScore - процент схожести
 */
function highlightSimilarityHeader(similarityScore) {
    const similarityDisplay = document.querySelector('.similarity-header');
    if (!similarityDisplay) return;

    if (similarityScore >= 80) {
        similarityDisplay.style.borderColor = '#10b981';
        similarityDisplay.style.background = 'linear-gradient(135deg, rgba(16, 185, 129, 0.1), rgba(33, 37, 41, 0.9))';
    } else if (similarityScore >= 60) {
        similarityDisplay.style.borderColor = '#f59e0b';
        similarityDisplay.style.background = 'linear-gradient(135deg, rgba(245, 158, 11, 0.1), rgba(33, 37, 41, 0.9))';
    }
}

/**
 * Плавный скролл к блоку с общими критериями (с центрированием)
 * @param {string} targetId - ID целевого блока
 */
function scrollToSharedSection(targetId) {
    const targetElement = document.getElementById(targetId);
    if (!targetElement) return;

    // Скролл с центрированием элемента
    targetElement.scrollIntoView({
        behavior: 'smooth',
        block: 'center',
        inline: 'center'
    });
}

/**
 * Обработчик клика по элементу с общими критериями
 * @param {Event} event - событие клика
 */
function handleCommonItemClick(event) {
    const target = event.currentTarget;
    const criterionType = target.dataset.criterionType;

    if (!criterionType) return;

    // Маппинг типов критериев на ID блоков
    const sectionMap = {
        'genres': 'shared-genres-section',
        'keywords': 'shared-keywords-section',
        'themes': 'shared-themes-section',
        'perspectives': 'shared-perspectives-section',
        'developers': 'shared-developers-section',
        'game_modes': 'shared-game-modes-section',
        'engines': 'shared-engines-section'
    };

    const targetId = sectionMap[criterionType];
    if (targetId) {
        scrollToSharedSection(targetId);
    }
}

/**
 * Добавление обработчиков для элементов с общими критериями
 */
function setupCommonItemsClickHandlers() {
    const commonItems = document.querySelectorAll('[data-criterion-type]');
    commonItems.forEach(item => {
        item.addEventListener('click', handleCommonItemClick);
        // Добавляем указатель мыши для понимания, что элемент кликабельный
        item.style.cursor = 'pointer';
    });
}

/**
 * Функция для раскрытия/скрытия ключевых слов
 */
function toggleKeywords() {
    const hiddenKeywords = document.getElementById('hidden-keywords');
    const toggleText = document.getElementById('keywords-toggle-text');
    const toggleIcon = document.getElementById('keywords-toggle-icon');

    if (!hiddenKeywords || !toggleText || !toggleIcon) return;

    if (hiddenKeywords.style.display === 'none') {
        hiddenKeywords.style.display = 'block';
        toggleText.textContent = 'Show less';
        toggleIcon.className = 'bi bi-chevron-up';
    } else {
        hiddenKeywords.style.display = 'none';
        toggleText.textContent = 'Show more';
        toggleIcon.className = 'bi bi-chevron-down';
    }
}

/**
 * Инициализация логики для ключевых слов
 */
function initKeywordsToggle() {
    const keywordsContainer = document.querySelector('.keywords-container');
    if (!keywordsContainer) return;

    const allKeywords = keywordsContainer.querySelectorAll('.badge');
    if (allKeywords.length > 10) {
        const visibleKeywords = keywordsContainer.querySelectorAll('.keywords-collapsible .badge');
        if (visibleKeywords.length > 6) {
            const hiddenSection = document.getElementById('hidden-keywords');
            if (hiddenSection) {
                const keywordsArray = Array.from(visibleKeywords);
                keywordsArray.slice(6).forEach(keyword => {
                    hiddenSection.appendChild(keyword);
                });
            }
        }
    }
}

/**
 * Центрирование жанров на карточках сравнения
 */
function centerGenresOnCards() {
    const comparisonCards = document.querySelectorAll('.game-comparison-card');
    comparisonCards.forEach(card => {
        const genresContainer = card.querySelector('.game-genres');
        if (genresContainer) {
            genresContainer.style.display = 'flex';
            genresContainer.style.flexWrap = 'wrap';
            genresContainer.style.justifyContent = 'center';
            genresContainer.style.alignItems = 'center';
            genresContainer.style.gap = '0.5rem';
            genresContainer.style.marginTop = '1rem';
            genresContainer.style.textAlign = 'center';
        }
    });
}

/**
 * Главная функция инициализации при загрузке страницы
 */
document.addEventListener('DOMContentLoaded', function() {
    // Получаем процент схожести из data-атрибута или переменной
    const similarityScore = parseFloat(document.body.dataset.similarityScore || '0');

    // Инициализация всех функций
    initTooltips();
    highlightSimilarityHeader(similarityScore);
    initKeywordsToggle();
    centerGenresOnCards();
    setupCommonItemsClickHandlers();
});

// Экспортируем функции для глобального доступа
window.toggleKeywords = toggleKeywords;
window.scrollToSharedSection = scrollToSharedSection;