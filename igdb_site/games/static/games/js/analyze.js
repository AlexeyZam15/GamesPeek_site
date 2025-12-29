// games/static/games/js/analyze.js

class GameAnalyzerUI {
    constructor(options = {}) {
        this.options = {
            gameId: options.gameId || null,
            apiUrl: options.apiUrl || '',
            ...options
        };

        this.elements = {};
        this.currentTab = '';
        this.currentMode = 'combined'; // Всегда combined
        this.init();
    }

    /* ============================================
       INITIALIZATION METHODS
       ============================================ */

    init() {
        this.cacheElements();
        this.getCurrentState();
        this.bindEvents();
        this.setupTooltips();
        this.handleScroll();
        this.loadUrlParams();
        this.forceTextAlignmentFix();

        const urlParams = new URLSearchParams(window.location.search);
        if (urlParams.get('cleared') === '1') {
            console.log('Results were cleared, refreshing UI...');
            this.showMessage('✅ Несохранённые результаты успешно очищены.', 'success');

            setTimeout(() => {
                this.removeUrlParam('cleared');
            }, 3000);
        }

        if (urlParams.get('keyword_added') === '1') {
            this.showMessage('✅ Keyword added successfully!', 'success');
            setTimeout(() => {
                this.removeUrlParam('keyword_added');
            }, 3000);
        }

        console.log('Game Analyzer UI initialized in COMBINED mode');
    }

    removeUrlParam(param) {
        try {
            const url = new URL(window.location);
            url.searchParams.delete(param);
            window.history.replaceState({}, '', url);
        } catch (e) {
            console.error('Failed to remove URL param:', e);
        }
    }

    cacheElements() {
        this.elements = {
            modeButtons: document.querySelectorAll('.mode-btn'),
            analyzeForm: document.getElementById('analyze-form'),
            tabSelect: document.getElementById('analyze-tab-select'),
            newKeywordInput: document.getElementById('new-keyword-input'),
            addKeywordButton: document.getElementById('add-keyword-button'),
            highlightToggle: document.getElementById('highlight-toggle'),
            analyzeTabs: document.getElementById('analyzeTabs'),
            analyzeTabLinks: document.querySelectorAll('#analyzeTabs .nav-link'),
            analyzeTabPanes: document.querySelectorAll('.tab-pane'),
            copyTextBtn: document.getElementById('copy-text-btn'),
            findHighlightsBtn: document.getElementById('find-highlights-btn'),
            scrollToTopBtn: document.querySelector('.scroll-to-top'),
            statusBar: document.querySelector('.status-bar'),
            currentModeDisplay: document.getElementById('current-mode-display'),
            currentTabDisplay: document.getElementById('current-tab-display'),
            currentTabChars: document.getElementById('current-tab-chars'),
            tabInput: document.getElementById('analyze-tab-input'),
            modeInput: document.getElementById('analyze-mode-input'),
            analyzeButton: document.getElementById('analyze-button'),
            saveButton: document.getElementById('save-button'),
            clearResultsBtn: document.getElementById('clear-results-btn'),
            backToGameBtn: document.getElementById('back-to-game-btn')
        };

        console.log('Elements cached for COMBINED mode');
        console.log('- Form:', this.elements.analyzeForm ? 'found' : 'NOT FOUND');
        console.log('- Analyze button:', this.elements.analyzeButton ? 'found' : 'NOT FOUND');
    }

    getCurrentState() {
        const activeTabLink = document.querySelector('#analyzeTabs .nav-link.active');
        if (activeTabLink) {
            const href = activeTabLink.getAttribute('href');
            this.currentTab = href ? href.substring(1) : '';
        } else if (this.elements.analyzeTabLinks.length > 0) {
            const firstTab = this.elements.analyzeTabLinks[0];
            const href = firstTab.getAttribute('href');
            this.currentTab = href ? href.substring(1) : '';
        }

        this.currentMode = 'combined'; // Всегда combined
    }

    /* ============================================
       EVENT HANDLING METHODS
       ============================================ */

    bindEvents() {
        console.log('=== BINDING EVENTS FOR COMBINED MODE ===');

        this.bindTabSelect();
        this.bindHighlightToggle();
        this.bindAnalyzeButton();
        this.bindSaveButton();
        this.bindAddKeywordButton();
        this.bindOtherButtons();
        this.bindBootstrapTabs();

        console.log('=== ALL EVENTS BOUND ===');
    }

    bindTabSelect() {
        if (!this.elements.tabSelect) {
            console.error('Tab select not found');
            return;
        }

        this.elements.tabSelect.addEventListener('change', (e) => {
            const tabName = e.target.value;
            console.log(`Tab select changed to: ${tabName}`);

            if (this.elements.tabInput) {
                this.elements.tabInput.value = tabName;
            }

            this.switchTabByName(tabName);
        });
    }

    bindHighlightToggle() {
        if (!this.elements.highlightToggle) {
            console.warn('Highlight toggle not found');
            return;
        }

        this.elements.highlightToggle.addEventListener('change', (e) => {
            console.log(`Highlight toggle: ${e.target.checked}`);
            this.handleHighlightToggle(e);
        });
    }

    bindAnalyzeButton() {
        console.log('=== BINDING ANALYZE BUTTON ===');

        if (!this.elements.analyzeButton) {
            console.error('ANALYZE BUTTON NOT FOUND!');
            this.showMessage('Error: Analyze button not found. Please refresh page.', 'error');
            return;
        }

        console.log('Found analyze button:', this.elements.analyzeButton);

        const originalButton = this.elements.analyzeButton;
        const newButton = originalButton.cloneNode(true);
        originalButton.parentNode.replaceChild(newButton, originalButton);
        this.elements.analyzeButton = newButton;

        this.elements.analyzeButton.addEventListener('click', (e) => {
            console.log('=== ANALYZE BUTTON CLICKED (COMBINED MODE) ===');
            e.preventDefault();
            e.stopPropagation();

            if (!this.elements.analyzeForm) {
                console.error('Form element not found');
                this.showMessage('Error: Form not found', 'error');
                return;
            }

            this.updateHiddenFields();

            const csrfToken = document.querySelector('[name="csrfmiddlewaretoken"]');
            if (!csrfToken) {
                console.error('CSRF token not found');
                this.showMessage('Error: Security token missing. Please refresh page.', 'error');
                return;
            }

            let analyzeField = this.elements.analyzeForm.querySelector('input[name="analyze"]');
            if (!analyzeField) {
                console.log('Creating hidden analyze field');
                analyzeField = document.createElement('input');
                analyzeField.type = 'hidden';
                analyzeField.name = 'analyze';
                analyzeField.value = 'true';
                this.elements.analyzeForm.appendChild(analyzeField);
            }

            const originalHTML = this.elements.analyzeButton.innerHTML;
            this.elements.analyzeButton.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Analyzing...';
            this.elements.analyzeButton.disabled = true;

            console.log('=== SUBMITTING FORM FOR COMBINED ANALYSIS ===');
            console.log('- Mode: combined (always)');

            setTimeout(() => {
                try {
                    console.log('Attempting form submission...');
                    this.elements.analyzeForm.submit();
                } catch (error) {
                    console.error('Error submitting form:', error);
                    this.showMessage('Error submitting form: ' + error.message, 'error');

                    this.elements.analyzeButton.innerHTML = originalHTML;
                    this.elements.analyzeButton.disabled = false;
                }
            }, 300);
        });

        console.log('Analyze button bound successfully');
    }

    bindSaveButton() {
        if (!this.elements.saveButton) {
            console.log('Save button not found (may be hidden)');
            return;
        }

        const newButton = this.elements.saveButton.cloneNode(true);
        this.elements.saveButton.parentNode.replaceChild(newButton, this.elements.saveButton);
        this.elements.saveButton = newButton;

        this.elements.saveButton.addEventListener('click', (e) => {
            e.preventDefault();
            this.handleSaveResults(e);
        });
    }

    bindAddKeywordButton() {
        const addButton = document.getElementById('add-keyword-button');
        const keywordInput = document.getElementById('new-keyword-input');

        if (!addButton || !keywordInput) {
            console.log('Add keyword button or input not found');
            return;
        }

        console.log('Found add keyword button and input');

        keywordInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                this.handleAddKeyword();
            }
        });

        addButton.addEventListener('click', (e) => {
            e.preventDefault();
            this.handleAddKeyword();
        });

        console.log('Add keyword button bound successfully');
    }

    bindOtherButtons() {
        if (this.elements.copyTextBtn) {
            this.elements.copyTextBtn.addEventListener('click', () => this.copyText());
        }

        if (this.elements.findHighlightsBtn) {
            this.elements.findHighlightsBtn.addEventListener('click', () => this.scrollToFirstHighlight());
        }

        if (this.elements.scrollToTopBtn) {
            this.elements.scrollToTopBtn.addEventListener('click', () => this.scrollToTop());
            window.addEventListener('scroll', () => this.handleScroll());
        }

        this.bindClearResultsButton();

        document.addEventListener('click', (e) => {
            if (e.target.classList.contains('found-item-badge')) {
                e.preventDefault();
                this.scrollToHighlight(e.target.dataset.name);
            }
        });
    }

    bindClearResultsButton() {
        const clearButton = document.getElementById('clear-results-btn');
        if (!clearButton) {
            console.log('Clear results button not found');
            return;
        }

        console.log('Found clear results button:', clearButton);

        const originalButton = clearButton;
        const newButton = originalButton.cloneNode(true);
        originalButton.parentNode.replaceChild(newButton, originalButton);
        const clearBtn = newButton;

        clearBtn.addEventListener('click', async (e) => {
            e.preventDefault();
            e.stopPropagation();

            if (!confirm('Are you sure you want to clear unsaved analysis results?\n\nThis action cannot be undone.')) {
                console.log('Clear cancelled by user');
                return;
            }

            const originalHTML = clearBtn.innerHTML;
            clearBtn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Clearing...';
            clearBtn.disabled = true;

            console.log('Clearing analysis results...');

            const clearUrl = clearBtn.getAttribute('href');

            if (!clearUrl) {
                console.error('Clear URL not found');
                this.showMessage('Error: Clear URL not found', 'error');
                this.resetClearButton(clearBtn, originalHTML);
                return;
            }

            console.log('Clear URL:', clearUrl);

            try {
                const response = await fetch(clearUrl, {
                    method: 'GET',
                    headers: {
                        'X-Requested-With': 'XMLHttpRequest',
                        'Accept': 'application/json',
                    },
                    credentials: 'same-origin'
                });

                console.log('Clear response status:', response.status);

                if (response.ok) {
                    const data = await response.json();
                    console.log('Clear response data:', data);

                    if (data.success) {
                        this.showMessage('✅ ' + data.message, 'success');

                        if (data.redirect_url) {
                            setTimeout(() => {
                                window.location.href = data.redirect_url;
                            }, 1500);
                        } else {
                            setTimeout(() => {
                                window.location.reload();
                            }, 1500);
                        }
                    } else {
                        throw new Error(data.message || 'Clear error');
                    }
                } else {
                    throw new Error(`HTTP error: ${response.status}`);
                }
            } catch (error) {
                console.error('Error clearing results:', error);
                this.showMessage(`❌ Error clearing results: ${error.message}`, 'error');
                this.resetClearButton(clearBtn, originalHTML);
            }
        });

        console.log('Clear results button bound successfully');
    }

    resetClearButton(button, originalHTML) {
        if (button) {
            button.innerHTML = originalHTML;
            button.disabled = false;
        }
    }

    bindBootstrapTabs() {
        if (!this.elements.analyzeTabLinks || this.elements.analyzeTabLinks.length === 0) {
            return;
        }

        this.elements.analyzeTabLinks.forEach(link => {
            link.addEventListener('shown.bs.tab', (e) => {
                const href = e.target.getAttribute('href');
                if (href) {
                    const tabName = href.substring(1);
                    this.onTabSwitch(tabName);
                }
            });
        });
    }

    updateHiddenFields() {
        console.log('=== UPDATING HIDDEN FIELDS ===');

        if (this.elements.tabSelect && this.elements.tabInput) {
            const selectedTab = this.elements.tabSelect.value;
            this.elements.tabInput.value = selectedTab;
            console.log(`Tab input updated: ${selectedTab} → ${this.elements.tabInput.value}`);
        }

        if (this.elements.modeInput) {
            this.elements.modeInput.value = 'combined';
            console.log(`Mode input set to: combined (always)`);
        }

        console.log('Hidden fields update complete');
    }

    handleHighlightToggle(e) {
        console.log(`handleHighlightToggle: ${e.target.checked ? 'enabling' : 'disabling'} highlighting`);

        e.preventDefault();
        e.stopPropagation();

        const isEnabled = e.target.checked;

        this.showMessage(isEnabled ? 'Highlighting enabled...' : 'Highlighting disabled...', 'info');

        if (this.elements.analyzeForm) {
            let toggleInput = this.elements.analyzeForm.querySelector('input[name="highlight_toggle_only"]');
            if (!toggleInput) {
                toggleInput = document.createElement('input');
                toggleInput.type = 'hidden';
                toggleInput.name = 'highlight_toggle_only';
                this.elements.analyzeForm.appendChild(toggleInput);
            }
            toggleInput.value = '1';

            let highlightInput = this.elements.analyzeForm.querySelector('input[name="highlight_toggle"]');
            if (!highlightInput) {
                highlightInput = document.createElement('input');
                highlightInput.type = 'hidden';
                highlightInput.name = 'highlight_toggle';
                this.elements.analyzeForm.appendChild(highlightInput);
            }
            highlightInput.value = isEnabled ? 'on' : 'off';

            console.log('Toggle fields added to form');

            this.showMessage('Now click "Analyze Text" to apply highlighting changes', 'info');
        } else {
            console.error('Cannot handle highlight toggle: form not found');
            this.showMessage('Error: Cannot toggle highlighting. Form not found.', 'error');
        }
    }

    handleAddKeyword() {
        const keywordInput = document.getElementById('new-keyword-input');
        const addButton = document.getElementById('add-keyword-button');

        if (!keywordInput || !addButton) {
            console.error('Add keyword elements not found');
            return;
        }

        const keyword = keywordInput.value.trim();
        const gameId = document.getElementById('game-id').value;

        if (!keyword) {
            this.showMessage('Please enter a keyword', 'warning');
            keywordInput.focus();
            return;
        }

        if (!gameId) {
            this.showMessage('Error: Game ID not found', 'error');
            return;
        }

        console.log(`Adding keyword "${keyword}" to game ${gameId}`);

        const originalButtonText = addButton.innerHTML;
        const originalButtonState = addButton.disabled;

        addButton.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Adding...';
        addButton.disabled = true;

        const formData = new FormData();
        formData.append('add_keyword', 'true');
        formData.append('new_keyword', keyword);

        const csrfToken = this.getCSRFToken();
        if (csrfToken) {
            formData.append('csrfmiddlewaretoken', csrfToken);
        }

        fetch(this.elements.analyzeForm ? this.elements.analyzeForm.action : window.location.href, {
            method: 'POST',
            body: formData,
            headers: {
                'X-Requested-With': 'XMLHttpRequest'
            }
        })
        .then(response => {
            if (response.redirected) {
                window.location.href = response.url;
                return null;
            }
            return response.json();
        })
        .then(data => {
            if (!data) return; // Redirect handled

            if (data.success) {
                this.showMessage(`✅ Keyword "${keyword}" added successfully!`, 'success');

                keywordInput.value = '';

                this.updateCurrentKeywordsList(data.keyword_id, data.keyword_name);

                this.refreshFoundElementsList('keywords', {
                    id: data.keyword_id,
                    name: data.keyword_name,
                    is_new: true
                });

                keywordInput.focus();
            } else {
                throw new Error(data.error || 'Failed to add keyword');
            }
        })
        .catch(error => {
            console.error('Error adding keyword:', error);
            this.showMessage(`❌ Error adding keyword: ${error.message}`, 'error');
        })
        .finally(() => {
            addButton.innerHTML = originalButtonText;
            addButton.disabled = originalButtonState;
        });
    }

    getCSRFToken() {
        const csrfInput = document.querySelector('[name="csrfmiddlewaretoken"]');
        return csrfInput ? csrfInput.value : null;
    }

    updateCurrentKeywordsList(keywordId, keywordName) {
        const currentKeywordsContainer = document.querySelector('#current-items-container .current-items-list');

        if (!currentKeywordsContainer) {
            console.log('Current keywords container not found');
            return;
        }

        const newKeywordElement = document.createElement('span');
        newKeywordElement.className = 'current-item';
        newKeywordElement.textContent = keywordName;

        currentKeywordsContainer.prepend(newKeywordElement);

        this.updateKeywordCount('current');
    }

    refreshFoundElementsList(category, item) {
        const foundItemsContainer = document.getElementById('found-items-container');
        if (!foundItemsContainer) return;

        let categoryElement = foundItemsContainer.querySelector(`.found-items-category[data-category="${category}"]`);

        if (!categoryElement) {
            categoryElement = document.createElement('div');
            categoryElement.className = 'found-items-category';
            categoryElement.dataset.category = category;

            const categoryTitle = document.createElement('h6');
            categoryTitle.innerHTML = `${this.capitalizeFirstLetter(category)} (1)`;
            categoryElement.appendChild(categoryTitle);

            const itemsList = document.createElement('div');
            itemsList.className = 'found-items-list';
            categoryElement.appendChild(itemsList);

            foundItemsContainer.prepend(categoryElement);
        }

        const categoryTitle = categoryElement.querySelector('h6');
        const currentCount = categoryElement.querySelectorAll('.found-item-badge').length + 1;
        categoryTitle.innerHTML = `${this.capitalizeFirstLetter(category)} (${currentCount})`;

        const itemsList = categoryElement.querySelector('.found-items-list');
        const newBadge = document.createElement('span');

        let badgeClass = 'bg-secondary';
        if (category === 'keywords') badgeClass = 'bg-warning text-dark';
        else if (category === 'genres') badgeClass = 'bg-success';
        else if (category === 'themes') badgeClass = 'bg-danger';
        else if (category === 'perspectives') badgeClass = 'bg-primary';
        else if (category === 'game_modes') badgeClass = 'bg-purple';

        newBadge.className = `badge ${badgeClass} found-item-badge`;
        newBadge.dataset.name = item.name;
        newBadge.dataset.category = category;
        newBadge.setAttribute('data-bs-toggle', 'tooltip');
        newBadge.setAttribute('title', `New ${category} (added manually) - Click to scroll to highlight`);
        newBadge.innerHTML = `${item.name} <i class="bi bi-plus-circle ms-1"></i>`;

        itemsList.appendChild(newBadge);

        if (window.bootstrap) {
            new bootstrap.Tooltip(newBadge);
        }
    }

    capitalizeFirstLetter(string) {
        return string.charAt(0).toUpperCase() + string.slice(1);
    }

    updateKeywordCount(type) {
        if (type === 'current') {
            const categoryHeader = document.querySelector('#current-items-container .current-data-category h6');
            if (categoryHeader) {
                const currentCount = categoryHeader.parentElement.querySelectorAll('.current-item').length;
                categoryHeader.innerHTML = `Keywords (${currentCount})`;
            }
        }
    }

    /* ============================================
       TAB & MODE MANAGEMENT
       ============================================ */

    loadUrlParams() {
        const urlParams = new URLSearchParams(window.location.search);
        const tab = urlParams.get('tab');

        if (tab) {
            this.switchTabByName(tab);
        }

        // Mode всегда 'combined', игнорируем параметр mode
    }

    switchTabByName(tabName) {
        console.log(`switchTabByName called with: ${tabName}`);

        const tabLink = document.querySelector(`#analyzeTabs a[href="#${tabName}"]`);
        if (tabLink) {
            console.log(`Found tab link for: ${tabName}`);

            try {
                if (window.bootstrap) {
                    const tab = new bootstrap.Tab(tabLink);
                    tab.show();
                    console.log(`Tab ${tabName} shown successfully`);
                } else {
                    this.manualTabSwitch(tabName);
                }
            } catch (error) {
                console.error('Error showing tab:', error);
                this.manualTabSwitch(tabName);
            }
        } else {
            console.error(`Tab link not found for: ${tabName}`);
            this.showMessage(`Error: Tab ${tabName} not found.`, 'error');
        }
    }

    manualTabSwitch(tabName) {
        const tabLinks = document.querySelectorAll('#analyzeTabs .nav-link');
        const tabPanes = document.querySelectorAll('.tab-pane');

        tabLinks.forEach(link => {
            link.classList.remove('active');
            link.setAttribute('aria-selected', 'false');
        });

        tabPanes.forEach(pane => {
            pane.classList.remove('show', 'active');
        });

        const targetLink = document.querySelector(`#analyzeTabs a[href="#${tabName}"]`);
        const targetPane = document.getElementById(tabName);

        if (targetLink && targetPane) {
            targetLink.classList.add('active');
            targetLink.setAttribute('aria-selected', 'true');
            targetPane.classList.add('show', 'active');
            this.onTabSwitch(tabName);
        }
    }

    onTabSwitch(tabName) {
        if (this.currentTab === tabName) return;

        if (this.elements.tabSelect) {
            this.elements.tabSelect.value = tabName;
        }

        if (this.elements.tabInput) {
            this.elements.tabInput.value = tabName;
        }

        this.updateTabDisplay(tabName);

        this.currentTab = tabName;

        setTimeout(() => {
            this.forceTextAlignmentFix();
            this.setupTooltips();
        }, 10);

        this.updateUrlParam('tab', tabName);
    }

    updateTabDisplay(tabName) {
        if (this.elements.currentTabDisplay && this.elements.currentTabChars) {
            const tabLink = document.querySelector(`#analyzeTabs a[href="#${tabName}"]`);
            if (tabLink) {
                const label = tabLink.textContent.match(/[^\(]+/)[0].trim();
                const chars = tabLink.querySelector('.badge').textContent;

                this.elements.currentTabDisplay.textContent = label;
                this.elements.currentTabChars.textContent = chars;
            }
        }
    }

    /* ============================================
       TEXT ALIGNMENT FIX METHODS
       ============================================ */

    forceTextAlignmentFix() {
        const activePane = document.querySelector(`#${this.currentTab}.tab-pane.active`);
        if (!activePane) return;

        const textContent = activePane.querySelector('.text-content');
        if (!textContent) return;

        textContent.style.cssText = `
            text-align: left !important;
            margin: 0 !important;
            padding: 0 !important;
            position: static !important;
            transform: none !important;
            display: block !important;
            vertical-align: top !important;
        `;

        const paragraphs = textContent.querySelectorAll('p');
        paragraphs.forEach(p => {
            p.style.cssText = `
                text-align: left !important;
                margin-bottom: 1.25em !important;
                margin-top: 0 !important;
            `;
        });

        const headings = textContent.querySelectorAll('h1, h2, h3, h4, h5, h6');
        headings.forEach(h => {
            h.style.cssText = 'text-align: left !important;';
        });

        const divs = textContent.querySelectorAll('div');
        divs.forEach(div => {
            if (div.classList.contains('text-center') ||
                div.classList.contains('align-center') ||
                div.classList.contains('mx-auto') ||
                div.style.textAlign === 'center') {
                div.style.cssText = `
                    text-align: left !important;
                    margin: 0 !important;
                    display: block !important;
                `;
            }
        });
    }

    /* ============================================
       ACTION METHODS
       ============================================ */

    copyText() {
        const activePane = document.querySelector(`#${this.currentTab}.tab-pane.active`);
        if (!activePane) {
            this.showMessage('No text available to copy', 'warning');
            return;
        }

        const textContent = activePane.querySelector('.text-content');
        if (!textContent) {
            this.showMessage('No text available to copy', 'warning');
            return;
        }

        const clone = textContent.cloneNode(true);

        clone.querySelectorAll('.highlight-info').forEach(el => el.remove());

        clone.querySelectorAll('mark').forEach(mark => {
            mark.replaceWith(mark.textContent);
        });

        const text = clone.textContent || clone.innerText;

        if (!text.trim()) {
            this.showMessage('No text available to copy', 'warning');
            return;
        }

        navigator.clipboard.writeText(text.trim())
            .then(() => {
                this.showMessage('Text copied to clipboard!', 'success');
            })
            .catch(err => {
                console.error('Clipboard error:', err);
                this.showMessage('Failed to copy text', 'error');
            });
    }

    scrollToHighlight(elementName) {
        const activePane = document.querySelector(`#${this.currentTab}.tab-pane.active`);
        if (!activePane) return;

        const highlights = activePane.querySelectorAll(`[data-element-name="${elementName}"]`);
        if (highlights.length > 0) {
            let targetHighlight = highlights[0];
            for (let i = 0; i < highlights.length; i++) {
                const rect = highlights[i].getBoundingClientRect();
                if (rect.top >= 0 && rect.bottom <= window.innerHeight) {
                    targetHighlight = highlights[i];
                    break;
                }
            }

            targetHighlight.scrollIntoView({
                behavior: 'smooth',
                block: 'center'
            });

            highlights.forEach(h => {
                h.classList.add('highlight-pulse');
                setTimeout(() => h.classList.remove('highlight-pulse'), 2000);
            });
        }
    }

    scrollToFirstHighlight() {
        const activePane = document.querySelector(`#${this.currentTab}.tab-pane.active`);
        if (!activePane) return;

        const firstHighlight = activePane.querySelector('mark');
        if (firstHighlight) {
            firstHighlight.scrollIntoView({
                behavior: 'smooth',
                block: 'center'
            });

            firstHighlight.classList.add('highlight-pulse');
            setTimeout(() => {
                firstHighlight.classList.remove('highlight-pulse');
            }, 2000);
        } else {
            this.showMessage('No highlighted elements found', 'info');
        }
    }

    scrollToTop() {
        window.scrollTo({
            top: 0,
            behavior: 'smooth'
        });
    }

    handleSaveResults(e) {
        console.log('handleSaveResults called for COMBINED mode');
        e.preventDefault();

        if (!confirm('Are you sure you want to save the analysis results to the game database?\n\nThis will add new elements (genres, themes, keywords, etc.) to the game.')) {
            console.log('Save cancelled by user');
            return;
        }

        if (!this.elements.analyzeForm) {
            console.error('Cannot save: form not found');
            this.showMessage('Error: Form not found. Cannot save results.', 'error');
            return;
        }

        console.log('Starting save process...');

        const saveButton = this.elements.analyzeForm.querySelector('button[name="save_results"]');
        if (saveButton) {
            const originalHTML = saveButton.innerHTML;
            saveButton.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Saving...';
            saveButton.disabled = true;
            saveButton.dataset.originalHTML = originalHTML;
        }

        this.showMessage('Saving results to database...', 'info');

        const saveInput = document.createElement('input');
        saveInput.type = 'hidden';
        saveInput.name = 'save_results';
        saveInput.value = 'true';
        this.elements.analyzeForm.appendChild(saveInput);

        console.log('Submitting form for save...');

        setTimeout(() => {
            try {
                this.elements.analyzeForm.submit();
                console.log('Form submitted successfully for save');
            } catch (error) {
                console.error('Error submitting form for save:', error);
                this.showMessage('❌ Error submitting form: ' + error.message, 'error');

                if (saveButton && saveButton.dataset.originalHTML) {
                    saveButton.innerHTML = saveButton.dataset.originalHTML;
                    saveButton.disabled = false;
                }
            }
        }, 100);
    }

    /* ============================================
       UTILITY METHODS
       ============================================ */

    setupTooltips() {
        const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
        tooltipTriggerList.map(tooltipTriggerEl => {
            const existingTooltip = window.bootstrap && bootstrap.Tooltip.getInstance(tooltipTriggerEl);
            if (existingTooltip) {
                existingTooltip.dispose();
            }
            if (window.bootstrap) {
                return new bootstrap.Tooltip(tooltipTriggerEl, {
                    trigger: 'hover focus'
                });
            }
        });

        const activePane = document.querySelector(`#${this.currentTab}.tab-pane.active`);
        if (activePane) {
            const highlights = activePane.querySelectorAll('mark');
            highlights.forEach(highlight => {
                if (!highlight.hasAttribute('data-bs-toggle')) {
                    const elementName = highlight.dataset.elementName || 'Found element';
                    highlight.setAttribute('data-bs-toggle', 'tooltip');
                    highlight.setAttribute('data-bs-title', elementName);
                    highlight.setAttribute('data-bs-placement', 'top');

                    if (window.bootstrap) {
                        const existingTooltip = bootstrap.Tooltip.getInstance(highlight);
                        if (existingTooltip) {
                            existingTooltip.dispose();
                        }
                        new bootstrap.Tooltip(highlight, {
                            trigger: 'hover focus',
                            placement: 'top'
                        });
                    }
                }
            });
        }
    }

    handleScroll() {
        if (!this.elements.scrollToTopBtn) return;

        if (window.scrollY > 300) {
            this.elements.scrollToTopBtn.classList.add('visible');
        } else {
            this.elements.scrollToTopBtn.classList.remove('visible');
        }

        if (this.elements.statusBar) {
            if (window.scrollY > 100) {
                this.elements.statusBar.classList.add('visible');
            } else {
                this.elements.statusBar.classList.remove('visible');
            }
        }
    }

    updateUrlParam(key, value) {
        try {
            const url = new URL(window.location);
            url.searchParams.set(key, value);
            window.history.replaceState({}, '', url);
        } catch (e) {
            console.error('Failed to update URL:', e);
        }
    }

    showLoading(message = 'Processing...') {
        if (this.elements.statusBar) {
            this.elements.statusBar.classList.add('visible');
            const statusMessage = this.elements.statusBar.querySelector('.status-message');
            if (statusMessage) {
                statusMessage.innerHTML =
                    `<i class="bi bi-hourglass-split text-warning me-2"></i> ${message}`;
            }
        }
    }

    hideLoading() {
        if (this.elements.statusBar) {
            setTimeout(() => {
                this.elements.statusBar.classList.remove('visible');
            }, 1000);
        }
    }

    showMessage(text, type = 'info') {
        const oldAlerts = document.querySelectorAll('.analyzer-alert');
        oldAlerts.forEach(alert => {
            if (window.bootstrap) {
                const bsAlert = bootstrap.Alert.getInstance(alert);
                if (bsAlert) {
                    bsAlert.close();
                } else {
                    alert.remove();
                }
            } else {
                alert.remove();
            }
        });

        const alert = document.createElement('div');
        alert.className = `alert alert-${type} alert-dismissible fade show analyzer-alert position-fixed`;
        alert.style.cssText = `
            top: 20px;
            right: 20px;
            z-index: 1060;
            min-width: 300px;
            max-width: 500px;
            box-shadow: 0 8px 32px rgba(0,0,0,0.3);
            animation: slideInRight 0.3s ease;
        `;

        const icons = {
            'success': 'bi-check-circle',
            'error': 'bi-x-circle',
            'warning': 'bi-exclamation-triangle',
            'info': 'bi-info-circle'
        };

        alert.innerHTML = `
            <div class="d-flex align-items-center">
                <i class="bi ${icons[type] || 'bi-info-circle'} me-3 fs-5 text-${type}"></i>
                <div class="flex-grow-1">${text}</div>
                <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
            </div>
        `;

        document.body.appendChild(alert);

        if (window.bootstrap) {
            new bootstrap.Alert(alert);
        }

        if (type !== 'error') {
            setTimeout(() => {
                if (alert.parentNode) {
                    if (window.bootstrap) {
                        const bsAlert = bootstrap.Alert.getInstance(alert);
                        if (bsAlert) {
                            bsAlert.close();
                        }
                    } else {
                        alert.remove();
                    }
                }
            }, 5000);
        }
    }
}

/* ============================================
   GLOBAL INITIALIZATION
   ============================================ */

document.addEventListener('DOMContentLoaded', function() {
    try {
        const analyzer = new GameAnalyzerUI();
        window.gameAnalyzer = analyzer;

        setTimeout(() => {
            analyzer.forceTextAlignmentFix();
        }, 100);

        window.addEventListener('resize', () => {
            setTimeout(() => {
                analyzer.forceTextAlignmentFix();
            }, 100);
        });

        const tabLinks = document.querySelectorAll('#analyzeTabs .nav-link');
        tabLinks.forEach(link => {
            link.addEventListener('hidden.bs.tab', () => {
                setTimeout(() => {
                    analyzer.setupTooltips();
                }, 50);
            });
        });

    } catch (error) {
        console.error('Failed to initialize Game Analyzer UI:', error);
        const errorAlert = document.createElement('div');
        errorAlert.className = 'alert alert-danger alert-dismissible fade show position-fixed';
        errorAlert.style.cssText = `
            top: 20px;
            left: 50%;
            transform: translateX(-50%);
            z-index: 1060;
            min-width: 300px;
            max-width: 500px;
            text-align: center;
        `;
        errorAlert.innerHTML = `
            <i class="bi bi-x-circle me-2"></i>
            Failed to initialize analyzer. Please refresh the page.
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        `;
        document.body.appendChild(errorAlert);
    }
});

const style = document.createElement('style');
style.textContent = `
    @keyframes slideInRight {
        from {
            transform: translateX(100%);
            opacity: 0;
        }
        to {
            transform: translateX(0);
            opacity: 1;
        }
    }

    @keyframes highlightPulse {
        0% {
            box-shadow: 0 0 0 0 rgba(255, 107, 53, 0.7);
        }
        70% {
            box-shadow: 0 0 0 10px rgba(255, 107, 53, 0);
        }
        100% {
            box-shadow: 0 0 0 0 rgba(255, 107, 53, 0);
        }
    }

    .highlight-pulse {
        animation: highlightPulse 1s ease-in-out;
    }

    .scroll-to-top {
        opacity: 0;
        transition: opacity 0.3s ease, transform 0.3s ease;
    }

    .scroll-to-top.visible {
        opacity: 1;
    }

    .status-bar {
        transition: transform 0.3s ease;
    }
`;
document.head.appendChild(style);