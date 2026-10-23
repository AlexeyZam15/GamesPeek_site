// games/static/games/js/game_list/game_list_filters.js
// Сбор параметров фильтров, отправка (Apply Filters / Apply Sort),
// random-seed и очистки (Clear All).

// Накопленные очистки (Clear All): ключ параметра -> '' (явно пустой).
// Живёт на уровне модуля, чтобы initFormSubmit (Apply) могла её сбросить,
// а initClearButtons — накапливать и применять поверх window.location.search.
// Сбрасывается, когда updateUrlParams обновил URL и все ключи уже пусты в URL,
// либо при явном Apply (пользователь выбрал значения — старые очистки неактуальны).
const pendingClearedParams = new Map();

// ===== ХЕЛПЕРЫ ДЛЯ СБОРА ПАРАМЕТРОВ И ОТПРАВКИ =====

/**
 * Генерирует новый случайный seed для сортировки "random".
 * 8 символов base36 — достаточно для стабильного ключа MD5 на сервере.
 */
function generateRandomSeed() {
    return Math.random().toString(36).slice(2, 10);
}

/**
 * Возвращает базовые параметры: копию текущего URL + накопленные очистки.
 * Это ключевое изменение: раньше параметры строились с нуля из DOM,
 * из-за чего любой параметр, не отражённый в DOM как :checked
 * (например, выбранный жанр search_g=31, отображаемый бейджем,
 * либо q — текстовый поиск), терялся при Apply Sort.
 */
function getBaseParams() {
    const params = new URLSearchParams(window.location.search);

    // Применяем накопленные очистки поверх URL: явно очищенные ключи
    // должны остаться пустыми, даже если в URL было старое значение.
    if (pendingClearedParams.size > 0) {
        pendingClearedParams.forEach(function (value, key) {
            params.set(key, value);
        });
    }

    return params;
}

/**
 * Утилита: устанавливает параметр, только если в DOM есть выбранные значения.
 * Если selectedValues пуст — параметр НЕ трогаем (сохраняем значение из URL,
 * которое могло быть выбрано через бейдж и не отразиться в чекбоксе).
 */
function setParamIfSelected(params, key, selectedValues) {
    if (selectedValues && selectedValues.length > 0) {
        params.set(key, selectedValues.join(','));
    }
    // Если selectedValues пуст — оставляем как есть (из URL/очисток).
}

/**
 * Собирает все параметры фильтров и сортировки.
 * БАЗА — текущий URL (+ pendingClearedParams), поверх неё накладываются
 * значения из DOM. Параметры, которых нет в DOM как :checked, НЕ удаляются —
 * это предотвращает сброс фильтров при смене сортировки.
 *
 * НЕ добавляет page и seed — это делает вызывающий код,
 * чтобы поведение seed'а (новый или сохранённый) зависело от действия.
 */
function collectFilterParams() {
    const params = getBaseParams();

    // ===== SEARCH FILTERS (с префиксом search_) =====

    const searchPlatforms = document.querySelectorAll('.search-platform-checkbox:checked');
    setParamIfSelected(params, 'search_p', Array.from(searchPlatforms).map(cb => cb.value));

    const searchGameTypes = document.querySelectorAll('.search-game-type-checkbox:checked');
    setParamIfSelected(params, 'search_gt', Array.from(searchGameTypes).map(cb => cb.value));

    const searchGenres = document.querySelectorAll('.search-genre-checkbox:checked');
    setParamIfSelected(params, 'search_g', Array.from(searchGenres).map(cb => cb.value));

    const searchKeywords = document.querySelectorAll('.search-keyword-checkbox:checked');
    setParamIfSelected(params, 'search_k', Array.from(searchKeywords).map(cb => cb.value));

    const searchThemes = document.querySelectorAll('.search-theme-checkbox:checked');
    setParamIfSelected(params, 'search_t', Array.from(searchThemes).map(cb => cb.value));

    const searchPerspectives = document.querySelectorAll('.search-perspective-checkbox:checked');
    setParamIfSelected(params, 'search_pp', Array.from(searchPerspectives).map(cb => cb.value));

    const searchGameModes = document.querySelectorAll('.search-game-mode-checkbox:checked');
    setParamIfSelected(params, 'search_gm', Array.from(searchGameModes).map(cb => cb.value));

    const searchEngines = document.querySelectorAll('.search-engine-checkbox:checked');
    setParamIfSelected(params, 'search_e', Array.from(searchEngines).map(cb => cb.value));

    const searchYearStart = document.getElementById('search-manual-year-start')?.value;
    const searchYearEnd = document.getElementById('search-manual-year-end')?.value;
    if (searchYearStart && searchYearStart !== '') params.set('search_ys', searchYearStart);
    if (searchYearEnd && searchYearEnd !== '') params.set('search_ye', searchYearEnd);

    // ===== SIMILARITY FILTERS (без префикса) =====

    const similarityGenres = document.querySelectorAll('.genre-checkbox:checked');
    setParamIfSelected(params, 'g', Array.from(similarityGenres).map(cb => cb.value));

    const similarityKeywords = document.querySelectorAll('.keyword-checkbox:checked');
    setParamIfSelected(params, 'k', Array.from(similarityKeywords).map(cb => cb.value));

    const similarityThemes = document.querySelectorAll('.theme-checkbox:checked');
    setParamIfSelected(params, 't', Array.from(similarityThemes).map(cb => cb.value));

    const similarityPerspectives = document.querySelectorAll('.perspective-checkbox:checked');
    setParamIfSelected(params, 'pp', Array.from(similarityPerspectives).map(cb => cb.value));

    const similarityGameModes = document.querySelectorAll('.game-mode-checkbox:checked');
    setParamIfSelected(params, 'gm', Array.from(similarityGameModes).map(cb => cb.value));

    const similarityEngines = document.querySelectorAll('.engine-checkbox:checked');
    setParamIfSelected(params, 'e', Array.from(similarityEngines).map(cb => cb.value));

    // ===== SOURCE GAME / FIND_SIMILAR =====

    const sourceGameInput = document.getElementById('server-source-game-id');
    if (sourceGameInput && sourceGameInput.value) {
        params.set('source_game', sourceGameInput.value);
    }

    // find_similar: включаем, если есть similarity-критерии, source_game
    // или параметр уже был в URL.
    const hasSimilarity = similarityGenres.length > 0 || similarityKeywords.length > 0 ||
                          similarityThemes.length > 0 || similarityPerspectives.length > 0 ||
                          similarityGameModes.length > 0 || similarityEngines.length > 0 ||
                          (sourceGameInput && sourceGameInput.value) ||
                          params.get('find_similar') === '1';

    if (hasSimilarity) {
        params.set('find_similar', '1');
    }

    // ===== ТЕКСТОВЫЙ ПОИСК (q) =====
    // Раньше q терялся при Apply Sort. Сохраняем из URL, если поле ввода
    // поиска в навбаре не переопределило его.
    const navSearchInput = document.querySelector('form[action*="search_results"] input[name="q"]');
    if (navSearchInput && navSearchInput.value && navSearchInput.value.trim()) {
        params.set('q', navSearchInput.value.trim());
    }
    // Иначе q остаётся из URL (getBaseParams уже его скопировал).

    // ===== СОРТИРОВКА =====

    const sortSelect = document.querySelector('select[name="sort"]');
    if (sortSelect && sortSelect.value) {
        params.set('sort', sortSelect.value);
    }

    // page сбрасывается при любом применении фильтров/сортировки.
    params.set('page', '1');

    return params;
}

/**
 * Отправляет запрос со списком игр: через AJAX (если доступно) или обычной навигацией.
 * params уже содержит page=1 (устанавливается в collectFilterParams).
 */
function submitGameListWithParams(params) {
    // Гарантируем page=1 (на случай вызова из других мест).
    params.set('page', '1');

    const newUrl = window.location.pathname + '?' + params.toString();
    console.log('Final URL:', newUrl);

    if (typeof window.AjaxPagination !== 'undefined' && window.AjaxPagination.loadPage) {
        const ajaxUrl = '/ajax/load-games-page/?' + params.toString();
        window.AjaxPagination.loadPage(ajaxUrl, newUrl, '1');
    } else {
        window.location.href = newUrl;
    }
}

// ===== ФУНКЦИЯ ДЛЯ ОТПРАВКИ ФИЛЬТРОВ (Apply Filters) =====
function initFormSubmit() {
    console.log('Initializing form submit handler...');

    const form = document.getElementById('main-search-form');
    if (!form) {
        console.log('Form not found');
        return;
    }

    const applyButton = form.querySelector('.apply-filters-main');
    if (!applyButton) {
        console.log('Apply button (.apply-filters-main) not found');
        return;
    }

    // Удаляем старый обработчик, чтобы не было дублей
    const newButton = applyButton.cloneNode(true);
    applyButton.parentNode.replaceChild(newButton, applyButton);

    newButton.addEventListener('click', function (e) {
        e.preventDefault();
        console.log('Apply filters clicked - updating URL with filters');

        // ВАЖНО: НЕ вызываем pendingClearedParams.clear() вслепую.
        // collectFilterParams() берёт URL как базу и применяет pendingClearedParams
        // поверх. Явные очистки (Clear All) должны сохраняться, а выбранные
        // значения (из DOM или URL) — не теряться.

        const params = collectFilterParams();

        // При сортировке "random" КАЖДОЕ применение даёт новую перестановку.
        // Seed генерируется заново и не переиспользуется из URL.
        // (При листании пагинации seed, наоборот, сохраняется — это делает
        // ajax-pagination.js, копируя параметры из href ссылок пагинатора.)
        if (params.get('sort') === 'random') {
            params.set('seed', generateRandomSeed());
        } else {
            // При не-random сортировке seed больше не нужен.
            params.delete('seed');
        }

        // После явного применения фильтров накопленные очистки уже
        // материализованы в params (пустые значения), поэтому сбрасываем.
        pendingClearedParams.clear();

        submitGameListWithParams(params);
    });
}

// ===== ОБРАБОТКА КНОПКИ "APPLY SORT" (submit формы) =====
// Кнопка "Apply Sort" — <button type="submit"> внутри #main-search-form.
// Перехватываем submit через делегирование, чтобы обработчик переживал
// перерендер блока фильтров (refreshFiltersBlock пересоздаёт форму).
function initApplySortSubmit() {
    if (initApplySortSubmit.__bound) {
        return;
    }
    initApplySortSubmit.__bound = true;

    document.addEventListener('submit', function (e) {
        const form = e.target;
        if (!form || form.id !== 'main-search-form') {
            return;
        }

        e.preventDefault();
        console.log('Apply Sort submitted - updating URL with filters');

        // ВАЖНО: НЕ сбрасываем pendingClearedParams вслепую.
        // collectFilterParams() берёт URL как базу (+очистки) и накладывает DOM.
        // Это сохраняет выбранные фильтры (search_g=31 и т.п.) при смене сортировки.

        const params = collectFilterParams();

        // При random — новая перестановка на каждое нажатие Apply Sort.
        if (params.get('sort') === 'random') {
            params.set('seed', generateRandomSeed());
        } else {
            params.delete('seed');
        }

        // Очистки материализованы в params — сбрасываем накопленный список.
        pendingClearedParams.clear();

        submitGameListWithParams(params);
    });
}

// ===== ФУНКЦИИ ДЛЯ ОЧИСТКИ ФИЛЬТРОВ =====
function initClearButtons() {
    console.log('Initializing clear buttons...');

    if (initClearButtons.__bound) {
        return;
    }
    initClearButtons.__bound = true;

    // Сбрасываем накопленные очистки, когда URL уже содержит все
    // соответствующие пустые параметры.
    window.addEventListener('url-updated', function () {
        if (pendingClearedParams.size === 0) return;
        const urlParams = new URLSearchParams(window.location.search);
        let allApplied = true;
        pendingClearedParams.forEach(function (_value, key) {
            if (urlParams.get(key) !== '') {
                allApplied = false;
            }
        });
        if (allApplied) {
            console.log('ClearButtons: all pending clears applied in URL, resetting');
            pendingClearedParams.clear();
        }
    });

    const BADGE_SELECTOR =
        '.active-genre-tag, .search-active-genre-tag,' +
        ' .active-keyword-tag, .search-active-keyword-tag,' +
        ' .active-theme-tag, .search-active-theme-tag,' +
        ' .active-perspective-tag, .search-active-perspective-tag,' +
        ' .active-game-mode-tag, .search-active-game-mode-tag,' +
        ' .active-engine-tag, .search-active-engine-tag,' +
        ' .active-platform-tag, .search-active-platform-tag,' +
        ' .active-game-type-tag, .search-active-game-type-tag,' +
        ' .search-active-date-filter-tag';

    const PARAM_BADGE_SELECTORS = {
        'g': '.active-genre-tag, .search-active-genre-tag',
        'search_g': '.search-active-genre-tag',
        'k': '.active-keyword-tag, .search-active-keyword-tag',
        'search_k': '.search-active-keyword-tag',
        't': '.active-theme-tag, .search-active-theme-tag',
        'search_t': '.search-active-theme-tag',
        'pp': '.active-perspective-tag, .search-active-perspective-tag',
        'search_pp': '.search-active-perspective-tag',
        'gm': '.active-game-mode-tag, .search-active-game-mode-tag',
        'search_gm': '.search-active-game-mode-tag',
        'e': '.active-engine-tag, .search-active-engine-tag',
        'search_e': '.search-active-engine-tag',
        'p': '.active-platform-tag, .search-active-platform-tag',
        'search_p': '.search-active-platform-tag',
        'gt': '.active-game-type-tag, .search-active-game-type-tag',
        'search_gt': '.search-active-game-type-tag',
        'search_ys': '.search-active-date-filter-tag',
        'search_ye': '.search-active-date-filter-tag',
        'search_yr': '.search-active-date-filter-tag',
    };

    // Similarity-поля, которые сериализуются в URL при изменении критериев,
    // чтобы virtual-режим получил полный набор текущих критериев.
    const SIMILARITY_FIELDS = ['g', 'k', 't', 'pp', 'gm', 'e'];

    function collectBadgeValues(param) {
        if (!param) return [];
        const seen = new Set();
        const result = [];
        const safeParam = String(param).replace(/["\\]/g, '\\$&');
        const nodes = document.querySelectorAll(
            '[data-param="' + safeParam + '"][data-value]'
        );
        nodes.forEach(function (el) {
            if (!el.dataset || el.dataset.value === undefined) return;
            const v = String(el.dataset.value);
            if (!v) return;
            if (seen.has(v)) return;
            seen.add(v);
            result.push(v);
        });
        return result;
    }

    // Добавляет в params текущие значения similarity-полей из DOM-бейджей,
    // если их там ещё нет (и они не удаляются/не очищаются).
    // Нужно, чтобы при изменении критериев URL содержал полный набор
    // текущих критериев (virtual-режим: source не подставляется).
    function serializeCurrentSimilarityFields(params, excludedFields) {
        excludedFields = excludedFields || [];
        SIMILARITY_FIELDS.forEach(function (field) {
            if (excludedFields.indexOf(field) !== -1) return;
            if (pendingClearedParams.has(field)) return;
            if (params.has(field)) return;
            const vals = collectBadgeValues(field);
            if (vals.length > 0) {
                params.set(field, vals.join(','));
            }
        });
    }

    function removeBadgeFromDom(badge) {
        const parent = badge.parentElement;
        if (badge.parentNode) {
            badge.parentNode.removeChild(badge);
        }
        if (!parent) return;

        const remaining = parent.querySelectorAll(BADGE_SELECTOR);
        if (remaining.length === 0) {
            const activeBlock = parent.closest(
                '[id$="-active-genres"], [id$="-active-keywords"], ' +
                '[id$="-active-themes"], [id$="-active-perspectives"], ' +
                '[id$="-active-game-modes"], [id$="-active-engines"], ' +
                '[id$="-active-platforms"], [id$="-active-game-types"], ' +
                '[id="search-active-date-filter"]'
            );
            if (activeBlock) {
                const header = activeBlock.querySelector('.d-flex.justify-content-between');
                if (header) header.style.display = 'none';
            }
        }
    }

    function removeBadgesForParam(param) {
        if (!param) return;

        let nodes;
        const mapped = PARAM_BADGE_SELECTORS[param];
        if (mapped) {
            nodes = document.querySelectorAll(mapped);
        } else {
            const safeParam = String(param).replace(/["\\]/g, '\\$&');
            nodes = document.querySelectorAll(
                '[data-param="' + safeParam + '"][data-value]'
            );
        }

        if (!nodes.length) return;
        const badges = Array.from(nodes);
        badges.forEach(function (badge) {
            removeBadgeFromDom(badge);
        });
    }

    function reloadWithParams(params, explicitEmptyParams) {
        if (explicitEmptyParams) {
            Object.keys(explicitEmptyParams).forEach(function (p) {
                if (!params.has(p)) {
                    params.set(p, '');
                }
            });
        }
        params.set('page', '1');

        const basePath = window.location.pathname;
        const queryString = params.toString();
        const displayUrl = basePath + (queryString ? '?' + queryString : '');

        if (typeof window.AjaxPagination !== 'undefined' && window.AjaxPagination.loadPage) {
            const ajaxUrl = '/ajax/load-games-page/?' + params.toString();
            window.AjaxPagination.loadPage(ajaxUrl, displayUrl, '1');
        } else {
            window.location.href = displayUrl;
        }
    }

    function currentParams() {
        const params = new URLSearchParams(window.location.search);
        if (pendingClearedParams.size > 0) {
            pendingClearedParams.forEach(function (value, key) {
                params.set(key, value);
            });
        }
        return params;
    }

    document.addEventListener('click', function (e) {
        // 1) Кнопки Clear All с data-param (но без data-value).
        const clearBtn = e.target.closest('[data-param]');
        if (clearBtn && clearBtn.dataset.param && !clearBtn.dataset.value) {
            e.preventDefault();
            const paramsToClear = clearBtn.dataset.param.split(',');
            const keysToClear = [];
            paramsToClear.forEach(function (p) {
                const key = p.trim();
                if (!key) return;
                pendingClearedParams.set(key, '');
                keysToClear.push(key);
            });

            const params = currentParams();

            // Сериализуем остальные similarity-поля из DOM, чтобы virtual-режим
            // получил полный набор текущих критериев (кроме очищаемых).
            serializeCurrentSimilarityFields(params, keysToClear);

            const explicitEmpty = {};
            keysToClear.forEach(function (key) {
                params.delete(key);
                explicitEmpty[key] = true;
                removeBadgesForParam(key);
            });

            reloadWithParams(params, explicitEmpty);
            return;
        }

        // 2) Клик по бейджу выбранного значения.
        const badge = e.target.closest && e.target.closest(BADGE_SELECTOR);

        if (!badge) {
            return;
        }

        const param = badge.dataset.param;
        const value = badge.dataset.value;
        if (!param || !value) {
            return;
        }

        e.preventDefault();
        e.stopPropagation();

        const params = currentParams();
        const current = params.get(param);

        let baseValues;
        if (current !== null) {
            baseValues = current
                ? current.split(',').filter(function (v) { return v !== ''; })
                : [];
        } else {
            baseValues = collectBadgeValues(param);
        }

        const values = baseValues.filter(function (v) {
            return v !== String(value);
        });

        // Сериализуем остальные similarity-поля из DOM (кроме изменяемого).
        serializeCurrentSimilarityFields(params, [param]);

        removeBadgeFromDom(badge);

        if (values.length > 0) {
            params.set(param, values.join(','));
            reloadWithParams(params, null);
        } else {
            params.set(param, '');
            const explicitEmpty = {};
            explicitEmpty[param] = true;
            reloadWithParams(params, explicitEmpty);
        }
    });
}