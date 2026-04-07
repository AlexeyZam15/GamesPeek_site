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
 * Получает название категории из элемента
 * @param {Element} categoryNameElem - элемент с названием категории
 * @returns {string} - название категории
 */
function getCategoryName(categoryNameElem) {
    if (!categoryNameElem) return '';

    const iconSpan = categoryNameElem.querySelector('span');
    const icon = iconSpan ? iconSpan.textContent.trim() : '';

    let name = '';
    for (let node of categoryNameElem.childNodes) {
        if (node.nodeType === Node.TEXT_NODE && node.textContent.trim()) {
            name = node.textContent.trim();
            break;
        }
    }

    const iconToName = {
        '🎮': 'Genres',
        '🔑': 'Keywords',
        '🎭': 'Themes',
        '👁️': 'Perspectives',
        '🎯': 'Game Modes',
        '🏢': 'Developers'
    };

    if (icon && iconToName[icon]) {
        name = iconToName[icon];
    }

    return name || 'Unknown';
}

/**
 * Получает детали категории (common/total)
 * @param {Element} categoryNameElem - элемент с названием категории
 * @returns {string} - детали вида (7/7)
 */
function getCategoryDetails(categoryNameElem) {
    if (!categoryNameElem) return '';
    const small = categoryNameElem.querySelector('small');
    return small ? small.textContent.trim() : '';
}

/**
 * Получает процент вклада категории
 * @param {Element} contributionElem - элемент с вкладом
 * @returns {string} - процент вида +30.0%
 */
function getContribution(contributionElem) {
    if (!contributionElem) return '';
    return contributionElem.textContent.trim();
}

/**
 * Получает общие элементы категории
 * @param {Element} category - элемент категории
 * @returns {string} - список общих элементов через запятую
 */
function getSharedItems(category) {
    const sharedItemsGrid = category.querySelector('.shared-items-grid');
    if (!sharedItemsGrid) return '';

    const items = sharedItemsGrid.querySelectorAll('.shared-item');
    if (items.length === 0) return '';

    return Array.from(items).map(item => item.textContent.trim()).join(', ');
}

/**
 * Собирает текст из карточек сравнения
 * @returns {string} - отформатированный текст карточек
 */
function getComparisonCardsText() {
    const cards = document.querySelectorAll('.game-comparison-card');
    let result = '';

    cards.forEach((card) => {
        const gameName = card.querySelector('.game-title')?.textContent?.trim() || '';
        if (!gameName) return '';

        const similarityScore = card.querySelector('.similarity-value .text-warning')?.textContent?.trim() || '';
        const ratingElem = card.querySelector('.game-rating .text-warning');
        const rating = ratingElem ? ratingElem.textContent.trim() : '';
        const ratingCount = card.querySelector('.game-rating small')?.textContent?.trim() || '';

        const genres = Array.from(card.querySelectorAll('.game-genres .badge')).map(el => el.textContent.trim());
        const gameModes = Array.from(card.querySelectorAll('.game-modes .badge')).map(el => el.textContent.trim());
        const developers = Array.from(card.querySelectorAll('.game-developers .badge')).map(el => el.textContent.trim());
        const releaseDateElem = card.querySelector('.release-date small');
        const releaseDate = releaseDateElem ? releaseDateElem.textContent.trim() : '';

        result += `\n${'='.repeat(50)}\n`;
        result += `${gameName}\n`;
        if (similarityScore) result += `Similarity: ${similarityScore}\n`;
        if (rating) result += `${rating} ${ratingCount}\n`;
        if (genres.length) result += `Genres: ${genres.join(', ')}\n`;
        if (gameModes.length) result += `Game Modes: ${gameModes.join(', ')}\n`;
        if (developers.length) result += `Developers: ${developers.join(', ')}\n`;
        if (releaseDate) result += `Release Date: ${releaseDate}\n`;
    });

    return result;
}

/**
 * Собирает текст из Similarity Breakdown
 * @returns {string} - отформатированный текст
 */
function getSimilarityBreakdownText() {
    const categories = document.querySelectorAll('.similarity-category');
    if (categories.length === 0) return '';

    let result = `\n${'='.repeat(50)}\n`;
    result += `SIMILARITY BREAKDOWN\n`;
    result += `${'='.repeat(50)}\n`;

    let bonusText = '';
    let totalText = '';

    categories.forEach(category => {
        if (category.classList.contains('total-category')) {
            const totalScore = category.querySelector('.similarity-badge')?.textContent?.trim() || '';
            totalText = `\nTOTAL SIMILARITY: ${totalScore}\n`;
            return;
        }

        if (category.classList.contains('bonus-category')) {
            const contribution = getContribution(category.querySelector('.contribution-value'));
            bonusText = `\nBonus (multiple criteria) → ${contribution}\n`;
            return;
        }

        const categoryNameElem = category.querySelector('.category-name');
        const name = getCategoryName(categoryNameElem);
        const details = getCategoryDetails(categoryNameElem);
        const contribution = getContribution(category.querySelector('.contribution-value'));
        const sharedItems = getSharedItems(category);

        if (name) {
            result += `\n${name} ${details} → ${contribution}\n`;
            if (sharedItems) {
                result += `  Shared: ${sharedItems}\n`;
            }
        }
    });

    if (bonusText) result += bonusText;
    if (totalText) result += totalText;

    return result;
}

/**
 * Собирает текст из Shared секций
 * @returns {string} - отформатированный текст
 */
function getSharedSectionsText() {
    const sections = [
        { id: 'shared-genres-section', title: '🎮 Shared Genres' },
        { id: 'shared-themes-section', title: '🎭 Shared Themes' },
        { id: 'shared-developers-section', title: '🏢 Shared Developers' },
        { id: 'shared-perspectives-section', title: '👁️ Shared Perspectives' },
        { id: 'shared-game-modes-section', title: '🎯 Shared Game Modes' },
        { id: 'shared-keywords-section', title: '🔑 Shared Features' }
    ];

    let result = `\n${'='.repeat(50)}\n`;
    result += `SHARED ITEMS\n`;
    result += `${'='.repeat(50)}\n`;

    sections.forEach(section => {
        const sectionElement = document.getElementById(section.id);
        if (!sectionElement) return;

        const items = sectionElement.querySelectorAll('.shared-item');
        if (items.length === 0) return;

        const itemNames = Array.from(items).map(item => item.textContent.trim());

        result += `\n${section.title}\n`;
        result += `${itemNames.join(', ')}\n`;
    });

    return result;
}

/**
 * Копирует всю страницу в читаемом текстовом формате
 */
function copyComparisonAsText() {
    const similarityHeader = document.querySelector('.similarity-header');
    let similarityText = '';
    if (similarityHeader) {
        const score = similarityHeader.querySelector('h3')?.textContent?.trim() || '';
        const description = similarityHeader.querySelector('p')?.textContent?.trim() || '';
        if (score) similarityText = `${score}\n${description}\n`;
    }

    const cardsText = getComparisonCardsText();
    const breakdownText = getSimilarityBreakdownText();
    const sharedText = getSharedSectionsText();

    const finalText = similarityText + cardsText + breakdownText + sharedText;

    navigator.clipboard.writeText(finalText).then(() => {
        const copyBtn = document.getElementById('copyComparisonAsText');
        if (copyBtn) {
            const originalHtml = copyBtn.innerHTML;
            copyBtn.innerHTML = '<i class="bi bi-check-lg"></i> Copied!';
            setTimeout(() => {
                copyBtn.innerHTML = originalHtml;
            }, 2000);
        }
    });
}

/**
 * Главная функция инициализации при загрузке страницы
 */
document.addEventListener('DOMContentLoaded', function() {
    const similarityScore = parseFloat(document.body.dataset.similarityScore || '0');

    initTooltips();
    highlightSimilarityHeader(similarityScore);
    initKeywordsToggle();
    centerGenresOnCards();
    setupCommonItemsClickHandlers();

    const copyBtn = document.getElementById('copyComparisonAsText');
    if (copyBtn) {
        copyBtn.addEventListener('click', copyComparisonAsText);
    }
});

window.toggleKeywords = toggleKeywords;
window.scrollToSharedSection = scrollToSharedSection;