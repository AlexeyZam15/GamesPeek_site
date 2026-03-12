// games/static/games/js/analyze/keyword-handlers.js
/**
 * AJAX обработчики для добавления/удаления ключевых слов
 */

/* ============================================
   AJAX ADD KEYWORD HANDLER
   ============================================ */

export class AddKeywordHandler {
    constructor(analyzer) {
        this.analyzer = analyzer;
        this.gameId = this._getGameId();
    }

    _getGameId() {
        const gameIdElement = document.getElementById('game-id');
        if (gameIdElement && gameIdElement.value) {
            return gameIdElement.value;
        }
        const urlMatch = window.location.pathname.match(/\/games\/(\d+)\/analyze/);
        return urlMatch ? urlMatch[1] : null;
    }

    bind() {
        const addButton = document.getElementById('add-keyword-button');
        const keywordInput = document.getElementById('new-keyword-input');

        if (!addButton || !keywordInput) return;

        // Enter key
        keywordInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                this.handleAddKeyword();
            }
        });

        // Button click
        addButton.addEventListener('click', (e) => {
            e.preventDefault();
            this.handleAddKeyword();
        });
    }

    async handleAddKeyword() {
        const keywordInput = document.getElementById('new-keyword-input');
        const keyword = keywordInput ? keywordInput.value.trim() : '';

        if (!keyword) {
            this.analyzer.showMessage('Please enter a keyword', 'error');
            return;
        }

        const csrfToken = this.getCSRFToken();
        if (!csrfToken) {
            this.analyzer.showMessage('Security token missing', 'error');
            return;
        }

        if (!this.gameId) {
            this.analyzer.showMessage('Game ID not found', 'error');
            return;
        }

        const addButton = document.getElementById('add-keyword-button');
        const originalHTML = addButton ? addButton.innerHTML : '';
        if (addButton) {
            addButton.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Adding...';
            addButton.disabled = true;
        }

        try {
            const response = await fetch(`/games/${this.gameId}/analyze/add-keyword-ajax/`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': csrfToken,
                    'X-Requested-With': 'XMLHttpRequest'
                },
                body: JSON.stringify({
                    keyword: keyword,
                    tab: this.analyzer.currentTab || 'summary'
                })
            });

            const data = await response.json();

            if (data.success) {
                // Очищаем поле ввода
                keywordInput.value = '';

                // Показываем сообщение
                if (data.was_created) {
                    this.analyzer.showMessage(`✅ Created and added new keyword: "${keyword}"`, 'success');
                } else if (data.already_exists) {
                    this.analyzer.showMessage(`ℹ️ Keyword "${keyword}" already exists in game`, 'info');
                } else {
                    this.analyzer.showMessage(`✅ Keyword "${keyword}" added to game!`, 'success');
                }

                // Обновляем списки ключевых слов
                this.refreshCurrentKeywords();
            } else {
                this.analyzer.showMessage(`❌ ${data.message}`, 'error');
            }
        } catch (error) {
            console.error('Error adding keyword:', error);
            this.analyzer.showMessage('❌ Error adding keyword: ' + error.message, 'error');
        } finally {
            if (addButton) {
                addButton.innerHTML = originalHTML;
                addButton.disabled = false;
            }
        }
    }

    async refreshCurrentKeywords() {
        if (!this.gameId) return;

        try {
            const response = await fetch(`/games/${this.gameId}/analyze/current-keywords/`);
            const data = await response.json();

            if (data.success && data.keywords) {
                // Находим контейнер с ключевыми словами
                const allCategories = document.querySelectorAll('.current-data-category');
                let keywordsContainer = null;

                allCategories.forEach(container => {
                    const h6 = container.querySelector('h6');
                    if (h6 && h6.textContent.includes('Keywords')) {
                        keywordsContainer = container;
                    }
                });

                if (keywordsContainer) {
                    const itemsList = keywordsContainer.querySelector('.current-items-list');
                    if (itemsList) {
                        itemsList.innerHTML = data.keywords.map(kw =>
                            `<span class="current-item">${kw}</span>`
                        ).join('');
                    }

                    // Обновляем счетчик
                    const countBadge = keywordsContainer.querySelector('.badge');
                    if (countBadge) {
                        countBadge.textContent = data.count;
                    }
                }
            }
        } catch (error) {
            console.error('Error refreshing keywords:', error);
        }
    }

    getCSRFToken() {
        const csrfInput = document.querySelector('[name="csrfmiddlewaretoken"]');
        return csrfInput ? csrfInput.value : null;
    }
}


/* ============================================
   AJAX DELETE KEYWORD HANDLER
   ============================================ */

export class DeleteKeywordHandler {
    constructor(analyzer) {
        this.analyzer = analyzer;
        this.gameId = this._getGameId();
    }

    _getGameId() {
        const gameIdElement = document.getElementById('game-id');
        if (gameIdElement && gameIdElement.value) {
            return gameIdElement.value;
        }
        const urlMatch = window.location.pathname.match(/\/games\/(\d+)\/analyze/);
        return urlMatch ? urlMatch[1] : null;
    }

    bind() {
        const deleteButton = document.getElementById('delete-keyword-button');
        const keywordInput = document.getElementById('new-keyword-input');

        if (!deleteButton) return;

        deleteButton.addEventListener('click', (e) => {
            e.preventDefault();
            this.handleDeleteKeyword();
        });

        if (keywordInput) {
            keywordInput.addEventListener('keydown', (e) => {
                if (e.key === 'Enter' && e.shiftKey) {
                    e.preventDefault();
                    this.handleDeleteKeyword();
                }
            });
        }
    }

    async handleDeleteKeyword() {
        const keywordInput = document.getElementById('new-keyword-input');
        const keyword = keywordInput ? keywordInput.value.trim() : '';

        if (!keyword) {
            this.analyzer.showMessage('Please enter a keyword to delete', 'error');
            return;
        }

        if (!confirm(`Are you sure you want to remove keyword "${keyword}" from this game?`)) {
            return;
        }

        const csrfToken = this.getCSRFToken();
        if (!csrfToken) {
            this.analyzer.showMessage('Security token missing', 'error');
            return;
        }

        if (!this.gameId) {
            this.analyzer.showMessage('Game ID not found', 'error');
            return;
        }

        const deleteButton = document.getElementById('delete-keyword-button');
        const originalHTML = deleteButton ? deleteButton.innerHTML : '';
        if (deleteButton) {
            deleteButton.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Removing...';
            deleteButton.disabled = true;
        }

        try {
            const response = await fetch(`/games/${this.gameId}/analyze/delete-keyword-ajax/`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': csrfToken,
                    'X-Requested-With': 'XMLHttpRequest'
                },
                body: JSON.stringify({
                    keyword: keyword,
                    tab: this.analyzer.currentTab || 'summary'
                })
            });

            const data = await response.json();

            if (data.success) {
                // Очищаем поле ввода
                keywordInput.value = '';

                // Показываем сообщение
                this.analyzer.showMessage(`✅ ${data.message}`, 'success');

                if (data.popularity !== undefined) {
                    if (data.still_exists_in_db) {
                        this.analyzer.showMessage(`ℹ️ Keyword still used in ${data.popularity} other game(s)`, 'info', 3000);
                    } else {
                        this.analyzer.showMessage(`ℹ️ Keyword completely deleted from database`, 'info', 3000);
                    }
                }

                // Обновляем списки ключевых слов
                this.refreshCurrentKeywords();
            } else {
                this.analyzer.showMessage(`❌ ${data.message}`, 'error');
            }
        } catch (error) {
            console.error('Error deleting keyword:', error);
            this.analyzer.showMessage('❌ Error deleting keyword: ' + error.message, 'error');
        } finally {
            if (deleteButton) {
                deleteButton.innerHTML = originalHTML;
                deleteButton.disabled = false;
            }
        }
    }

    async refreshCurrentKeywords() {
        if (!this.gameId) return;

        try {
            const response = await fetch(`/games/${this.gameId}/analyze/current-keywords/`);
            const data = await response.json();

            if (data.success && data.keywords) {
                const allCategories = document.querySelectorAll('.current-data-category');
                let keywordsContainer = null;

                allCategories.forEach(container => {
                    const h6 = container.querySelector('h6');
                    if (h6 && h6.textContent.includes('Keywords')) {
                        keywordsContainer = container;
                    }
                });

                if (keywordsContainer) {
                    const itemsList = keywordsContainer.querySelector('.current-items-list');
                    if (itemsList) {
                        itemsList.innerHTML = data.keywords.map(kw =>
                            `<span class="current-item">${kw}</span>`
                        ).join('');
                    }

                    const countBadge = keywordsContainer.querySelector('.badge');
                    if (countBadge) {
                        countBadge.textContent = data.count;
                    }
                }
            }
        } catch (error) {
            console.error('Error refreshing keywords:', error);
        }
    }

    getCSRFToken() {
        const csrfInput = document.querySelector('[name="csrfmiddlewaretoken"]');
        return csrfInput ? csrfInput.value : null;
    }
}