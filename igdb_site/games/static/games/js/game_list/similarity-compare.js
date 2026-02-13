// games/static/games/js/game_list/similarity-compare.js
const SimilarityCompare = {
    init: function() {
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', () => this.initCompareButtons());
        } else {
            this.initCompareButtons();
        }

        // Слушаем обновление сетки игр
        document.addEventListener('games-grid-updated', () => {
            setTimeout(() => this.initCompareButtons(), 50);
        });

        // Наблюдаем за DOM изменениями
        const observer = new MutationObserver((mutations) => {
            let shouldInit = false;
            mutations.forEach(mutation => {
                if (mutation.addedNodes.length) {
                    mutation.addedNodes.forEach(node => {
                        if (node.nodeType === 1) {
                            if (node.classList && node.classList.contains('game-card-container')) {
                                shouldInit = true;
                            }
                            if (node.querySelector && node.querySelector('.game-card-container')) {
                                shouldInit = true;
                            }
                        }
                    });
                }
            });
            if (shouldInit) {
                setTimeout(() => this.initCompareButtons(), 50);
            }
        });

        observer.observe(document.body, {
            childList: true,
            subtree: true
        });
    },

    initCompareButtons: function() {
        // Проверяем, находимся ли мы в режиме похожих игр
        const isSimilarMode = this.isSimilarMode();

        if (!isSimilarMode) {
            // Удаляем все кнопки Compare если не в режиме похожих
            this.removeAllCompareButtons();
            return;
        }

        // Получаем source_game из скрытого поля
        const sourceGameId = this.getSourceGameId();

        if (!sourceGameId) {
            console.warn('Source game not found, cannot add Compare buttons');
            return;
        }

        // Находим все карточки игр
        const gameCards = document.querySelectorAll('.game-card-container');

        gameCards.forEach(card => {
            // Пропускаем если это исходная игра
            const gameId = this.getGameIdFromCard(card);
            if (gameId === sourceGameId) {
                return;
            }

            // Получаем процент схожести
            const similarity = this.getSimilarityFromCard(card);

            // Добавляем кнопку Compare если есть процент схожести
            if (similarity && parseFloat(similarity) > 0) {
                this.addCompareButton(card, sourceGameId, gameId);
            }
        });
    },

    isSimilarMode: function() {
        // Проверяем наличие индикатора режима похожих
        if (document.querySelector('.similarity-mode-indicator') !== null) {
            return true;
        }

        // Проверяем URL параметр
        if (window.location.search.includes('find_similar=1')) {
            return true;
        }

        // Проверяем скрытое поле source_game
        const sourceGameInput = document.getElementById('server-source-game-id');
        if (sourceGameInput && sourceGameInput.value) {
            return true;
        }

        // Проверяем window.similarityMap
        if (window.similarityMap && Object.keys(window.similarityMap).length > 0) {
            return true;
        }

        return false;
    },

    getSourceGameId: function() {
        // Пытаемся получить source_game_id из разных источников

        // 1. Из скрытого поля
        const sourceGameInput = document.getElementById('server-source-game-id');
        if (sourceGameInput && sourceGameInput.value) {
            return sourceGameInput.value;
        }

        // 2. Из window.sourceGameId
        if (window.sourceGameId) {
            return window.sourceGameId;
        }

        // 3. Из URL параметра
        const urlParams = new URLSearchParams(window.location.search);
        const sourceGameParam = urlParams.get('source_game');
        if (sourceGameParam) {
            return sourceGameParam;
        }

        return null;
    },

    getGameIdFromCard: function(card) {
        // Пытаемся получить ID игры из карточки

        // 1. Из data-game-id
        if (card.dataset.gameId) {
            return card.dataset.gameId;
        }

        // 2. Из ссылки на детальную страницу
        const detailLink = card.querySelector('a[href*="/games/"]');
        if (detailLink) {
            const match = detailLink.href.match(/\/games\/(\d+)\//);
            if (match) {
                return match[1];
            }
        }

        return null;
    },

    getSimilarityFromCard: function(card) {
        // Пытаемся получить процент схожести из карточки

        // 1. Из data-similarity
        if (card.dataset.similarity) {
            return card.dataset.similarity;
        }

        // 2. Из placeholder бейджа
        const badgePlaceholder = card.querySelector('.similarity-badge-placeholder');
        if (badgePlaceholder && badgePlaceholder.dataset.similarity) {
            return badgePlaceholder.dataset.similarity;
        }

        // 3. Из window.similarityMap
        const gameId = this.getGameIdFromCard(card);
        if (gameId && window.similarityMap && window.similarityMap[gameId]) {
            return window.similarityMap[gameId];
        }

        return null;
    },

    addCompareButton: function(card, sourceGameId, targetGameId) {
        // Проверяем, не добавлена ли уже кнопка
        if (card.querySelector('.compare-button-added')) {
            return;
        }

        // Находим футер карточки
        const footer = card.querySelector('.card-footer .d-grid');
        if (!footer) {
            return;
        }

        // Создаем URL для сравнения
        const compareUrl = this.buildCompareUrl(sourceGameId, targetGameId);

        // Создаем компактную кнопку
        const button = document.createElement('a');
        button.href = compareUrl;
        button.className = 'btn btn-sm btn-outline-warning compare-button-added';
        button.innerHTML = '<i class="bi bi-arrow-left-right"></i> Compare';
        button.setAttribute('data-bs-toggle', 'tooltip');
        button.setAttribute('data-bs-placement', 'top');
        button.setAttribute('title', `Compare with ${this.getSourceGameName()}`);

        // Добавляем кнопку в футер
        footer.appendChild(button);

        // Инициализируем тултип Bootstrap
        if (typeof bootstrap !== 'undefined' && bootstrap.Tooltip) {
            new bootstrap.Tooltip(button);
        }
    },

    removeAllCompareButtons: function() {
        const compareButtons = document.querySelectorAll('.compare-button-added');
        compareButtons.forEach(button => {
            // Уничтожаем тултип если есть
            if (typeof bootstrap !== 'undefined' && bootstrap.Tooltip && bootstrap.Tooltip.getInstance) {
                const tooltip = bootstrap.Tooltip.getInstance(button);
                if (tooltip) {
                    tooltip.dispose();
                }
            }
            button.remove();
        });
    },

    buildCompareUrl: function(sourceGameId, targetGameId) {
        // Строим URL для страницы сравнения
        return `/games/compare/${targetGameId}/?source_game=${sourceGameId}`;
    },

    getSourceGameName: function() {
        const sourceGameInput = document.getElementById('server-source-game-name');
        if (sourceGameInput && sourceGameInput.value) {
            return sourceGameInput.value;
        }
        return 'Source Game';
    },

    refresh: function() {
        this.removeAllCompareButtons();
        this.initCompareButtons();
    }
};

// Инициализация при загрузке страницы
SimilarityCompare.init();
window.SimilarityCompare = SimilarityCompare;