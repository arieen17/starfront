/**
 * UCR Star – Home page (frontend only, no backend)
 * Map centered on Riverside, CA. Search UCR Star = filter datasets; Go to location = geocode and pan map.
 */
(function () {
  "use strict";

  const RIVERSIDE_CENTER = [-117.3962, 33.9533]; // [lng, lat]
  const DEFAULT_ZOOM = 10;
  const MIN_ZOOM = 0;
  const MAX_ZOOM = 19;
  const NOMINATIM_URL = "https://nominatim.openstreetmap.org/search";
  const RIVERSIDE_EXTENT = [-117.7, 33.4, -114.1, 34.1];
  const UCR_STAR_API = "https://star.cs.ucr.edu";
  const MAX_EXTENT_SPAN_FIT = 25;

  let map = null;
  let datasets = [];
  let allDatasets = [];
  let searchMarker = null;
  let datasetExtentLayerReady = false;
  let regionsData = null;
  let currentDownloadScope = "visible";
  let currentDownloadRegion = null;
  const MAX_SUGGESTIONS = 10;
  const PLACE_TAG_REGEX = /^[a-zA-Z\s]+$/;
  const NAV_TAGS = ["Riverside", "OSM2015", "NE", "TIGER2018"];

  function getDatasetListUrl() {
    var base =
      document.querySelector("script[data-dataset-base]")?.dataset
        ?.datasetBase || "";
    if (base) return base + "dataset.json";
    var useApi =
      document.querySelector("script[data-dataset-api]")?.dataset?.datasetApi;
    if (useApi !== "false" && useApi !== "0") {
      return UCR_STAR_API + "/datasets";
    }
    return "data/dataset.json";
  }

  function getDatasetDetailsUrl(id) {
    if (!id) return null;
    return UCR_STAR_API + "/datasets/" + encodeURIComponent(id) + ".json";
  }

  function getDatasetGeometryUrl(name, mbr) {
    if (!name || !mbr || mbr.length < 4) return null;
    var q =
      "mbr=" +
      mbr[0] +
      "," +
      mbr[1] +
      "," +
      mbr[2] +
      "," +
      mbr[3];
    return (
      UCR_STAR_API +
      "/datasets/" +
      encodeURIComponent(name) +
      "/download.geojson.gz?" +
      q
    );
  }

  function createMap() {
    map = new maplibregl.Map({
      container: "map",
      style: "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json",
      center: RIVERSIDE_CENTER,
      zoom: DEFAULT_ZOOM,
      minZoom: MIN_ZOOM,
      maxZoom: MAX_ZOOM,
    });

    map.on("load", function () {
      if (!map.getSource("dataset-extent")) {
        map.addSource("dataset-extent", {
          type: "geojson",
          data: { type: "FeatureCollection", features: [] },
        });
        var layers = map.getStyle().layers;
        var firstSymbolLayerId = null;
        for (var i = 0; i < layers.length; i++) {
          if (layers[i].type === "symbol") {
            firstSymbolLayerId = layers[i].id;
            break;
          }
        }
        map.addLayer(
          {
            id: "dataset-extent-fill",
            type: "fill",
            source: "dataset-extent",
            paint: {
              "fill-color": "#1976d2",
              "fill-opacity": 0.2,
            },
          },
          firstSymbolLayerId,
        );
        map.addLayer(
          {
            id: "dataset-extent-line",
            type: "line",
            source: "dataset-extent",
            paint: {
              "line-color": "#1976d2",
              "line-width": 3,
            },
          },
          firstSymbolLayerId,
        );
        map.addSource("dataset-geometry", {
          type: "geojson",
          data: { type: "FeatureCollection", features: [] },
        });
        map.addLayer(
          {
            id: "dataset-geometry-fill",
            type: "fill",
            source: "dataset-geometry",
            filter: [
              "in",
              ["geometry-type"],
              ["literal", ["Polygon", "MultiPolygon"]],
            ],
            paint: {
              "fill-color": "#f5f5f5",
              "fill-opacity": 0.15,
              "fill-outline-color": "#1a1a1a",
              "fill-antialias": true,
            },
          },
          firstSymbolLayerId,
        );
        map.addLayer(
          {
            id: "dataset-geometry-line",
            type: "line",
            source: "dataset-geometry",
            filter: [
              "in",
              ["geometry-type"],
              ["literal", ["LineString", "MultiLineString"]],
            ],
            paint: {
              "line-color": "#1a1a1a",
              "line-width": 1.25,
              "line-join": "round",
              "line-cap": "round",
            },
          },
          firstSymbolLayerId,
        );
        map.addLayer(
          {
            id: "dataset-geometry-point",
            type: "circle",
            source: "dataset-geometry",
            filter: [
              "in",
              ["geometry-type"],
              ["literal", ["Point", "MultiPoint"]],
            ],
            paint: {
              "circle-radius": 3,
              "circle-color": "#1a1a1a",
              "circle-stroke-width": 1,
              "circle-stroke-color": "#fff",
            },
          },
          firstSymbolLayerId,
        );
        datasetExtentLayerReady = true;
        var card = document.getElementById("dataset-preview");
        if (card && !card.classList.contains("hidden")) {
          var idx = parseInt(card.getAttribute("data-index"), 10);
          if (!isNaN(idx) && datasets[idx]) updateDatasetExtentOnMap(datasets[idx]);
        }
      }
    });

    document.querySelector(".zoom-in")?.addEventListener("click", function () {
      map.setZoom(Math.min(MAX_ZOOM, map.getZoom() + 1));
    });
    document.querySelector(".zoom-out")?.addEventListener("click", function () {
      map.setZoom(Math.max(MIN_ZOOM, map.getZoom() - 1));
    });
    document
      .querySelector(".location-btn")
      ?.addEventListener("click", function () {
        if (!navigator.geolocation) return;
        navigator.geolocation.getCurrentPosition(
          function (pos) {
            var lng = pos.coords.longitude;
            var lat = pos.coords.latitude;
            map.flyTo({ center: [lng, lat], zoom: 14 });
            clearSearchMarker();
          },
          function () {},
        );
      });
  }

  function clearSearchMarker() {
    if (searchMarker) {
      searchMarker.remove();
      searchMarker = null;
    }
  }

  function getDatasetExtent(d) {
    if (!d) return null;
    var ext = d.extent;
    if (ext) {
      var minLon = ext[0] != null ? ext[0] : ext.minLon;
      var minLat = ext[1] != null ? ext[1] : ext.minLat;
      var maxLon = ext[2] != null ? ext[2] : ext.maxLon;
      var maxLat = ext[3] != null ? ext[3] : ext.maxLat;
      if (
        minLon != null &&
        minLat != null &&
        maxLon != null &&
        maxLat != null
      ) {
        return [minLon, minLat, maxLon, maxLat];
      }
    }
    if ((d.name || "").indexOf("Riverside/") === 0) {
      return RIVERSIDE_EXTENT.slice();
    }
    return null;
  }

  function extentToGeoJSON(ext) {
    if (!ext || ext.length < 4) return null;
    var minLon = ext[0];
    var minLat = ext[1];
    var maxLon = ext[2];
    var maxLat = ext[3];
    return {
      type: "Feature",
      properties: {},
      geometry: {
        type: "Polygon",
        coordinates: [
          [
            [minLon, minLat],
            [maxLon, minLat],
            [maxLon, maxLat],
            [minLon, maxLat],
            [minLon, minLat],
          ],
        ],
      },
    };
  }

  function updateDatasetExtentOnMap(d) {
    if (!map) return;
    if (!map.isStyleLoaded()) {
      map.once("style.load", function () {
        updateDatasetExtentOnMap(d);
      });
      return;
    }
    var source = map.getSource("dataset-extent");
    var geomSource = map.getSource("dataset-geometry");
    if (!source || !geomSource) {
      setTimeout(function () {
        updateDatasetExtentOnMap(d);
      }, 100);
      return;
    }
    geomSource.setData({ type: "FeatureCollection", features: [] });
    var ext = getDatasetExtent(d);
    if (ext) {
      var spanLon = ext[2] - ext[0];
      var spanLat = ext[3] - ext[1];
      var isSmallArea =
        spanLon <= MAX_EXTENT_SPAN_FIT && spanLat <= MAX_EXTENT_SPAN_FIT;
      if (isSmallArea && d.name) {
        var feature = extentToGeoJSON(ext);
        if (feature) {
          source.setData({
            type: "FeatureCollection",
            features: [feature],
          });
          map.fitBounds(
            [
              [ext[0], ext[1]],
              [ext[2], ext[3]],
            ],
            { padding: 40, duration: 500 },
          );
        }
        fetchDatasetGeometry(d.name, ext, function (geojson) {
          if (!map || !geomSource) return;
          var fc =
            geojson &&
            (geojson.type === "FeatureCollection"
              ? geojson
              : geojson.type === "Feature"
                ? { type: "FeatureCollection", features: [geojson] }
                : null);
          if (fc && fc.features && fc.features.length > 0) {
            geomSource.setData(fc);
            source.setData({ type: "FeatureCollection", features: [] });
          }
        });
      } else {
        var bboxFeature = extentToGeoJSON(ext);
        if (bboxFeature) {
          source.setData({
            type: "FeatureCollection",
            features: [bboxFeature],
          });
        }
      }
    } else {
      source.setData({ type: "FeatureCollection", features: [] });
    }
  }

  function clearDatasetExtentOnMap() {
    if (!map) return;
    var source = map.getSource("dataset-extent");
    if (source) source.setData({ type: "FeatureCollection", features: [] });
    var geomSource = map.getSource("dataset-geometry");
    if (geomSource)
      geomSource.setData({ type: "FeatureCollection", features: [] });
  }

  function fetchDatasetGeometry(name, mbr, onDone) {
    var url = getDatasetGeometryUrl(name, mbr);
    if (!url || !onDone) return;
    fetch(url)
      .then(function (r) {
        if (!r.ok) throw new Error(r.status);
        var ct = r.headers.get("content-type") || "";
        if (ct.indexOf("application/json") !== -1)
          return r.json();
        return r.arrayBuffer().then(function (buf) {
          if (buf.byteLength < 2) return null;
          var u8 = new Uint8Array(buf);
          if (u8[0] === 0x1f && u8[1] === 0x8b) {
            return new Promise(function (resolve, reject) {
              if (typeof DecompressionStream !== "undefined") {
                var ds = new DecompressionStream("gzip");
                new Response(buf).body.pipeThrough(ds).getReader().read().then(function (result) {
                  try {
                    var dec = new TextDecoder().decode(result.value);
                    resolve(JSON.parse(dec));
                  } catch (e) {
                    reject(e);
                  }
                }).catch(reject);
              } else {
                reject(new Error("gzip not supported"));
              }
            });
          }
          return JSON.parse(new TextDecoder().decode(buf));
        });
      })
      .then(function (geojson) {
        if (geojson && (geojson.features || geojson.type === "FeatureCollection"))
          onDone(geojson);
        else
          onDone(null);
      })
      .catch(function () {
        onDone(null);
      });
  }

  function goToLocation(query) {
    const q = (query || "").trim();
    if (!q) return;
    const url =
      NOMINATIM_URL + "?q=" + encodeURIComponent(q) + "&format=json&limit=1";
    fetch(url, {
      headers: { Accept: "application/json" },
    })
      .then(function (r) {
        return r.json();
      })
      .then(function (results) {
        if (!results || results.length === 0) return;
        const r = results[0];
        const lon = parseFloat(r.lon);
        const lat = parseFloat(r.lat);
        map.flyTo({ center: [lon, lat], zoom: 14 });
        clearSearchMarker();
        var el = document.createElement("div");
        el.className = "search-result-marker";
        searchMarker = new maplibregl.Marker({ element: el })
          .setLngLat([lon, lat])
          .addTo(map);
      })
      .catch(function () {});
  }

  function wireSearchToMap() {
    const input = document.getElementById("search-input");
    if (!input) return;
    input.addEventListener("keydown", function (e) {
      if (e.key === "Enter") {
        e.preventDefault();
        goToLocation(input.value);
      }
    });
  }

  function getOtherCategoryPrefixes() {
    var seen = {};
    var list = [];
    (allDatasets || []).forEach(function (d) {
      var name = (d.name || "").trim();
      if (!name) return;
      var prefix = name.indexOf("/") !== -1 ? name.split("/")[0] : name;
      if (seen[prefix]) return;
      seen[prefix] = true;
      if (NAV_TAGS.indexOf(prefix) === -1) list.push(prefix);
    });
    return list.sort();
  }

  function renderCategoryTags() {
    const container = document.getElementById("category-tags");
    if (!container) return;
    container.innerHTML = "";

    NAV_TAGS.forEach(function (tag) {
      var btn = document.createElement("button");
      btn.type = "button";
      btn.className = "category-btn";
      btn.dataset.category = tag;
      btn.setAttribute("aria-pressed", "false");
      var textSpan = document.createElement("span");
      textSpan.className = "category-btn-text";
      textSpan.textContent = tag;
      var clearSpan = document.createElement("span");
      clearSpan.className = "category-btn-clear";
      clearSpan.setAttribute("aria-label", "Deselect");
      clearSpan.innerHTML = "&times;";
      clearSpan.addEventListener("click", function (e) {
        e.stopPropagation();
        clearCategorySelection();
      });
      btn.appendChild(textSpan);
      btn.appendChild(clearSpan);
      btn.addEventListener("click", onTagClick);
      container.appendChild(btn);
    });

    var wrap = document.createElement("div");
    wrap.className = "categories-more-wrap";

    var moreBtn = document.createElement("button");
    moreBtn.type = "button";
    moreBtn.className = "category-btn";
    moreBtn.id = "more-category-btn";
    moreBtn.dataset.category = "";
    moreBtn.setAttribute("aria-pressed", "false");
    moreBtn.setAttribute("aria-haspopup", "listbox");
    moreBtn.setAttribute("aria-expanded", "false");
    var moreText = document.createElement("span");
    moreText.className = "category-btn-text";
    moreText.textContent = "More";
    var moreClear = document.createElement("span");
    moreClear.className = "category-btn-clear";
    moreClear.setAttribute("aria-label", "Deselect");
    moreClear.innerHTML = "&times;";
    moreClear.addEventListener("click", function (e) {
      e.stopPropagation();
      clearCategorySelection();
    });
    moreBtn.appendChild(moreText);
    moreBtn.appendChild(moreClear);
    moreBtn.addEventListener("click", function (e) {
      e.stopPropagation();
      toggleCategoriesDropdown();
    });

    var dropdown = document.createElement("div");
    dropdown.className = "categories-dropdown hidden";
    dropdown.setAttribute("role", "listbox");
    dropdown.id = "categories-dropdown";

    var allItem = document.createElement("button");
    allItem.type = "button";
    allItem.className = "category-dropdown-item";
    allItem.dataset.category = "";
    allItem.setAttribute("role", "option");
    allItem.textContent = "All";
    allItem.addEventListener("click", function () {
      setCategoryFromDropdown("");
    });
    dropdown.appendChild(allItem);

    getOtherCategoryPrefixes().forEach(function (prefix) {
      var item = document.createElement("button");
      item.type = "button";
      item.className = "category-dropdown-item";
      item.dataset.category = prefix;
      item.setAttribute("role", "option");
      item.textContent = prefix;
      item.addEventListener("click", function () {
        setCategoryFromDropdown(prefix);
      });
      dropdown.appendChild(item);
    });

    wrap.appendChild(moreBtn);
    wrap.appendChild(dropdown);
    container.appendChild(wrap);

    document.addEventListener("click", function (e) {
      if (
        document.getElementById("categories-dropdown") &&
        !wrap.contains(e.target)
      ) {
        closeCategoriesDropdown();
      }
    });
  }

  function clearCategorySelection() {
    document
      .querySelectorAll("#category-tags .category-btn")
      .forEach(function (b) {
        b.classList.remove("active");
        b.setAttribute("aria-pressed", "false");
        if (b.id === "more-category-btn") {
          b.dataset.category = "";
          var t = b.querySelector(".category-btn-text");
          if (t) t.textContent = "More";
        }
      });
    var moreBtn = document.getElementById("more-category-btn");
    if (moreBtn) moreBtn.classList.add("active");
    if (moreBtn) moreBtn.setAttribute("aria-pressed", "true");
    applyCategoryFilter(null);
  }

  function onTagClick(e) {
    if (e.target.classList.contains("category-btn-clear")) return;
    var tag = this.dataset.category;
    document
      .querySelectorAll("#category-tags .category-btn")
      .forEach(function (b) {
        var isActive = b.dataset.category === tag;
        b.classList.toggle("active", isActive);
        b.setAttribute("aria-pressed", isActive ? "true" : "false");
        if (b.id === "more-category-btn") {
          b.dataset.category = "";
          var t = b.querySelector(".category-btn-text");
          if (t) t.textContent = "More";
        }
      });
    applyCategoryFilter(tag || null);
    if (tag && PLACE_TAG_REGEX.test(tag)) goToLocation(tag);
  }

  function positionCategoriesDropdown() {
    var el = document.getElementById("categories-dropdown");
    var btn = document.getElementById("more-category-btn");
    if (!el || !btn) return;
    var r = btn.getBoundingClientRect();
    el.style.left = r.left + "px";
    el.style.top = (r.bottom + 6) + "px";
  }

  function toggleCategoriesDropdown() {
    var el = document.getElementById("categories-dropdown");
    var btn = document.getElementById("more-category-btn");
    if (!el || !btn) return;
    var isOpen = !el.classList.contains("hidden");
    if (isOpen) {
      closeCategoriesDropdown();
    } else {
      positionCategoriesDropdown();
      el.classList.remove("hidden");
      btn.setAttribute("aria-expanded", "true");
    }
  }

  function closeCategoriesDropdown() {
    var el = document.getElementById("categories-dropdown");
    var btn = document.getElementById("more-category-btn");
    if (el) el.classList.add("hidden");
    if (btn) btn.setAttribute("aria-expanded", "false");
  }

  function setCategoryFromDropdown(tag) {
    var moreBtn = document.getElementById("more-category-btn");
    if (!moreBtn) return;
    var moreText = moreBtn.querySelector(".category-btn-text");
    document
      .querySelectorAll("#category-tags .category-btn")
      .forEach(function (b) {
        if (b.id === "more-category-btn") {
          b.dataset.category = tag || "";
          if (moreText) moreText.textContent = tag ? tag : "More";
          b.classList.toggle("active", !!tag);
          b.setAttribute("aria-pressed", tag ? "true" : "false");
        } else {
          b.classList.remove("active");
          b.setAttribute("aria-pressed", "false");
        }
      });
    closeCategoriesDropdown();
    applyCategoryFilter(tag || null);
    if (tag && PLACE_TAG_REGEX.test(tag)) goToLocation(tag);
  }

  function loadDatasets() {
    var url = getDatasetListUrl();
    fetch(url)
      .then(function (r) {
        if (!r.ok) throw new Error("Could not load dataset list: " + r.status);
        return r.json();
      })
      .then(function (data) {
        allDatasets = data.datasets || [];
        renderCategoryTags();
        datasets = allDatasets.slice();
        applyCategoryFilter(null);
        renderDatasetList();
      })
      .catch(function (err) {
        console.warn("Datasets not loaded (no backend):", err.message);
        allDatasets = [];
        datasets = [];
        renderDatasetList();
      });
  }

  function applyCategoryFilter(activeCategory) {
    const searchEl = document.getElementById("search-input");
    const query = ((searchEl && searchEl.value) || "").toLowerCase().trim();

    if (!activeCategory && !query) {
      datasets = allDatasets.slice();
    } else {
      datasets = allDatasets.filter(function (d) {
        const name = (d.name || "").toLowerCase();
        const desc = (d.description || "").toLowerCase();
        const tags = (d.tags || "").toLowerCase();
        const text = name + " " + desc + " " + tags;
        const matchSearch = !query || text.indexOf(query) !== -1;
        const matchCategory =
          !activeCategory || categoryMatches(d, activeCategory);
        return matchSearch && matchCategory;
      });
    }
    renderDatasetList();
  }

  function categoryMatches(dataset, tag) {
    const name = dataset.name || "";
    return name === tag || name.startsWith(tag + "/");
  }

  function renderDatasetList() {
    const listEl = document.getElementById("dataset-list");
    if (!listEl) return;
    listEl.innerHTML = "";
    if (datasets.length === 0) {
      listEl.innerHTML =
        '<p style="padding: 16px; color: #666;">No datasets match.</p>';
      return;
    }
    datasets.forEach(function (d, i) {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "drawer-item";
      btn.setAttribute("data-index", String(i));
      const name = d.name || "Dataset #" + (i + 1);
      const desc = (d.description || "").slice(0, 80);
      btn.innerHTML =
        "<strong>" +
        escapeHtml(name) +
        "</strong>" +
        (desc
          ? "<span>" +
            escapeHtml(desc) +
            (d.description && d.description.length > 80 ? "…" : "") +
            "</span>"
          : "");
      btn.addEventListener("click", function () {
        selectDataset(i);
      });
      listEl.appendChild(btn);
    });
  }

  function escapeHtml(s) {
    const div = document.createElement("div");
    div.textContent = s;
    return div.innerHTML;
  }

  function formatSize(bytes) {
    if (bytes == null || bytes === undefined) return "";
    if (bytes >= 1e12) return (bytes / 1e12).toFixed(1) + " TB";
    if (bytes >= 1e9) return (bytes / 1e9).toFixed(1) + " GB";
    if (bytes >= 1e6) return (bytes / 1e6).toFixed(1) + " MB";
    if (bytes >= 1e3) return (bytes / 1e3).toFixed(1) + " KB";
    return String(bytes);
  }

  function formatCount(n) {
    if (n == null || n === undefined) return "";
    if (n >= 1e9) return (n / 1e9).toFixed(1) + " b";
    if (n >= 1e6) return (n / 1e6).toFixed(0) + " m";
    if (n >= 1e3) return (n / 1e3).toFixed(1) + " k";
    return n.toLocaleString();
  }

  function showPreviewCard(d, index) {
    var typeEl = document.getElementById("preview-type");
    var statsEl = document.getElementById("preview-stats");
    var descEl = document.getElementById("preview-desc");
    var card = document.getElementById("dataset-preview");
    if (typeEl) typeEl.textContent = "Type: " + (d.geometry_type || "—");
    if (statsEl) {
      var html = "";
      if (d.size != null)
        html += "<li><strong>Size:</strong> " + formatSize(d.size) + "</li>";
      if (d.num_features != null)
        html +=
          "<li><strong>Number of records:</strong> " +
          formatCount(d.num_features) +
          "</li>";
      if (d.num_points != null)
        html +=
          "<li><strong>Number of points:</strong> " +
          formatCount(d.num_points) +
          "</li>";
      html +=
        "<li><strong>Format:</strong> " + (d.format || "Geometry") + "</li>";
      statsEl.innerHTML = html;
    }
    if (descEl)
      descEl.textContent =
        (d.description || "").slice(0, 400) +
        (d.description && d.description.length > 400 ? "…" : "");
    if (card) {
      card.classList.remove("hidden");
      card.setAttribute("data-index", String(index));
    }
  }

  function selectDataset(index) {
    var d = datasets[index];
    if (!d) return;
    showPreviewCard(d, index);
    updateDatasetExtentOnMap(d);

    var detailsUrl = d._id ? getDatasetDetailsUrl(d._id) : null;
    if (detailsUrl) {
      fetch(detailsUrl)
        .then(function (r) {
          return r.ok ? r.json() : null;
        })
        .then(function (data) {
          if (!data || !data.dataset) return;
          var detail = data.dataset;
          if (detail.extent && Array.isArray(detail.extent))
            d.extent = detail.extent;
          if (detail.download_url) d.download_url = detail.download_url;
          if (detail.visualization) d.visualization = detail.visualization;
          if (detail.title) d.title = detail.title;
          if (detail.author) d.author = detail.author;
          updateDatasetExtentOnMap(d);
        })
        .catch(function () {});
    }
  }

  function getSuggestions(query) {
    const q = (query || "").toLowerCase().trim();
    if (!q) return [];
    return allDatasets
      .filter(function (d) {
        const name = (d.name || "").toLowerCase();
        const desc = (d.description || "").toLowerCase();
        return name.indexOf(q) !== -1 || desc.indexOf(q) !== -1;
      })
      .slice(0, MAX_SUGGESTIONS);
  }

  function showSuggestions(query) {
    const listEl = document.getElementById("search-suggestions");
    if (!listEl) return;
    const datasetMatches = getSuggestions(query);
    listEl.innerHTML = "";
    if (datasetMatches.length === 0) {
      listEl.classList.add("hidden");
      return;
    }
    listEl.classList.remove("hidden");
    datasetMatches.forEach(function (d) {
      var item = document.createElement("button");
      item.type = "button";
      item.className = "suggestion-item suggestion-dataset";
      item.textContent = d.name || "";
      item.dataset.name = d.name || "";
      item.setAttribute("role", "option");
      item.addEventListener("click", function () {
        var name = d.name || "";
        document.getElementById("search-input").value = name;
        listEl.classList.add("hidden");
        var active = document.querySelector(
          "#category-tags .category-btn.active",
        );
        applyCategoryFilter(active ? active.dataset.category || null : null);
        var idx = datasets.findIndex(function (x) {
          return (x.name || "") === name;
        });
        if (idx !== -1) selectDataset(idx);
      });
      listEl.appendChild(item);
    });
  }

  function hideSuggestions() {
    var listEl = document.getElementById("search-suggestions");
    if (listEl) listEl.classList.add("hidden");
  }

  function wireSearch() {
    const searchEl = document.getElementById("search-input");
    if (!searchEl) return;
    searchEl.addEventListener("input", function () {
      const active = document.querySelector(
        "#category-tags .category-btn.active",
      );
      applyCategoryFilter(active ? active.dataset.category || null : null);
      showSuggestions(searchEl.value);
    });
    searchEl.addEventListener("focus", function () {
      if (searchEl.value.trim()) showSuggestions(searchEl.value);
    });
    searchEl.addEventListener("blur", function () {
      setTimeout(hideSuggestions, 150);
    });
  }

  function openDrawer() {
    document.getElementById("dataset-drawer")?.classList.add("open");
    document.getElementById("drawer-backdrop")?.classList.add("open");
    document
      .getElementById("dataset-drawer")
      ?.setAttribute("aria-hidden", "false");
    document
      .getElementById("drawer-backdrop")
      ?.setAttribute("aria-hidden", "false");
  }

  function closeDrawer() {
    document.getElementById("dataset-drawer")?.classList.remove("open");
    document.getElementById("drawer-backdrop")?.classList.remove("open");
    document
      .getElementById("dataset-drawer")
      ?.setAttribute("aria-hidden", "true");
    document
      .getElementById("drawer-backdrop")
      ?.setAttribute("aria-hidden", "true");
  }

  function wireDrawer() {
    document
      .querySelector('.sidebar-btn[data-action="menu"]')
      ?.addEventListener("click", openDrawer);
    document
      .querySelector(".drawer-close")
      ?.addEventListener("click", closeDrawer);
    document
      .getElementById("drawer-backdrop")
      ?.addEventListener("click", closeDrawer);
  }

  function getShareLink() {
    var card = document.getElementById("dataset-preview");
    var base = window.location.origin + window.location.pathname;
    var datasetName = "";
    if (card && !card.classList.contains("hidden")) {
      var idx = parseInt(card.getAttribute("data-index"), 10);
      var d = datasets[idx];
      if (d && d.name) datasetName = d.name;
    }
    var hash = "";
    if (map) {
      var center = map.getCenter();
      if (center) {
        var lat = center.lat.toFixed(5);
        var lng = center.lng.toFixed(5);
        var zoom = Math.round(map.getZoom());
        hash = "#center=" + lat + "," + lng + "&zoom=" + zoom;
      }
    }
    var url =
      base + (datasetName ? "?" + encodeURIComponent(datasetName) : "") + hash;
    return url;
  }

  function openShareModal() {
    var link = getShareLink();
    var input = document.getElementById("share-permalink-input");
    if (input) input.value = link;
    var embedEl = document.getElementById("share-embed-input");
    if (embedEl)
      embedEl.value =
        '<iframe src="' +
        link +
        '" width="600" height="400" frameborder="0"></iframe>';
    var card = document.getElementById("dataset-preview");
    var citeEl = document.getElementById("share-cite-input");
    if (citeEl && card && !card.classList.contains("hidden")) {
      var idx = parseInt(card.getAttribute("data-index"), 10);
      var d = datasets[idx];
      var name = d && d.name ? d.name : "Dataset";
      citeEl.value = "UCR STAR. " + name + ". Retrieved from " + link;
    } else if (citeEl) citeEl.value = "UCR STAR. Retrieved from " + link;
    document.getElementById("share-modal-overlay")?.classList.remove("hidden");
    document
      .getElementById("share-modal-overlay")
      ?.setAttribute("aria-hidden", "false");
  }

  function closeShareModal() {
    document.getElementById("share-modal-overlay")?.classList.add("hidden");
    document
      .getElementById("share-modal-overlay")
      ?.setAttribute("aria-hidden", "true");
  }

  function copyShareInput(id) {
    var el = document.getElementById(id);
    if (!el || !el.value) return;
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(el.value).catch(function () {
        window.prompt("Copy:", el.value);
      });
    } else {
      window.prompt("Copy:", el.value);
    }
  }

  function wireShareModal() {
    document
      .querySelector(".share-modal-close")
      ?.addEventListener("click", closeShareModal);
    document
      .getElementById("share-modal-overlay")
      ?.addEventListener("click", function (e) {
        if (e.target === this) closeShareModal();
      });
    document.querySelectorAll(".share-tab").forEach(function (tab) {
      tab.addEventListener("click", function () {
        var t = this.dataset.tab;
        document.querySelectorAll(".share-tab").forEach(function (b) {
          b.classList.toggle("active", b.dataset.tab === t);
          b.setAttribute(
            "aria-selected",
            b.dataset.tab === t ? "true" : "false",
          );
        });
        document.querySelectorAll(".share-tab-pane").forEach(function (pane) {
          pane.classList.toggle("hidden", pane.id !== "share-" + t);
        });
      });
    });
    document
      .querySelector("#share-permalink .share-copy-btn")
      ?.addEventListener("click", function () {
        copyShareInput("share-permalink-input");
      });
    document
      .querySelector("#share-embed .share-copy-btn")
      ?.addEventListener("click", function () {
        copyShareInput("share-embed-input");
      });
    document
      .querySelector("#share-cite .share-copy-btn")
      ?.addEventListener("click", function () {
        copyShareInput("share-cite-input");
      });
  }

  function wirePreview() {
    document
      .querySelector(".preview-close")
      ?.addEventListener("click", function () {
        document.getElementById("dataset-preview")?.classList.add("hidden");
        clearDatasetExtentOnMap();
      });
    document
      .querySelector(".preview-share")
      ?.addEventListener("click", openShareModal);
    document
      .querySelector(".preview-download")
      ?.addEventListener("click", openDownloadModal);
  }

  function loadRegions() {
    fetch(UCR_STAR_API + "/regions.json")
      .then(function (r) {
        return r.ok ? r.json() : null;
      })
      .then(function (data) {
        if (data) regionsData = data;
      })
      .catch(function () {});
  }

  function getDownloadUrl(datasetName, format, scope, mbr, region) {
    if (!datasetName || !format) return null;
    var base =
      UCR_STAR_API +
      "/datasets/" +
      encodeURIComponent(datasetName) +
      "/download." +
      format;
    if (scope === "visible" && mbr) {
      return base + "?mbr=" + mbr[0] + "," + mbr[1] + "," + mbr[2] + "," + mbr[3];
    } else if (scope === "region" && region) {
      return base + "?region=" + encodeURIComponent(region);
    }
    return base;
  }

  function getMapBounds() {
    if (!map) return null;
    var bounds = map.getBounds();
    if (!bounds) return null;
    var sw = bounds.getSouthWest();
    var ne = bounds.getNorthEast();
    return [sw.lng, sw.lat, ne.lng, ne.lat];
  }

  function openDownloadModal() {
    var card = document.getElementById("dataset-preview");
    if (!card || card.classList.contains("hidden")) return;
    var idx = parseInt(card.getAttribute("data-index"), 10);
    var d = datasets[idx];
    if (!d || !d.name) return;
    currentDownloadScope = "visible";
    currentDownloadRegion = null;
    document.getElementById("download-modal-overlay")?.classList.remove("hidden");
    document
      .getElementById("download-modal-overlay")
      ?.setAttribute("aria-hidden", "false");
    updateDownloadScopeText();
    if (!regionsData) loadRegions();
  }

  function closeDownloadModal() {
    document.getElementById("download-modal-overlay")?.classList.add("hidden");
    document
      .getElementById("download-modal-overlay")
      ?.setAttribute("aria-hidden", "true");
    var regionSearch = document.getElementById("download-region-search");
    if (regionSearch) regionSearch.classList.add("hidden");
    var suggestions = document.getElementById("download-region-suggestions");
    if (suggestions) suggestions.classList.remove("visible");
  }

  function updateDownloadScopeText() {
    var textEl = document.getElementById("download-scope-text");
    if (!textEl) return;
    if (currentDownloadScope === "full") {
      textEl.textContent = "entire";
    } else if (currentDownloadScope === "visible") {
      textEl.textContent = "visible part";
    } else if (currentDownloadScope === "region") {
      textEl.textContent = "selected region";
    }
  }

  function handleDownloadFormat(format, label) {
    var card = document.getElementById("dataset-preview");
    if (!card || card.classList.contains("hidden")) return;
    var idx = parseInt(card.getAttribute("data-index"), 10);
    var d = datasets[idx];
    if (!d || !d.name) return;
    var url = null;
    var mbr = null;
    if (currentDownloadScope === "visible") {
      mbr = getMapBounds();
      url = getDownloadUrl(d.name, format, "visible", mbr, null);
    } else if (currentDownloadScope === "region" && currentDownloadRegion) {
      url = getDownloadUrl(d.name, format, "region", null, currentDownloadRegion);
    } else if (currentDownloadScope === "full") {
      url = getDownloadUrl(d.name, format, "full", null, null);
    }
    if (url) {
      window.open(url, "_blank");
      closeDownloadModal();
    }
  }

  function wireDownloadModal() {
    document
      .querySelector(".download-modal-close")
      ?.addEventListener("click", closeDownloadModal);
    document
      .getElementById("download-modal-overlay")
      ?.addEventListener("click", function (e) {
        if (e.target === this) closeDownloadModal();
      });
    document.querySelectorAll(".download-scope-btn").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var scope = this.dataset.scope;
        document.querySelectorAll(".download-scope-btn").forEach(function (b) {
          b.classList.remove("active");
        });
        this.classList.add("active");
        currentDownloadScope = scope;
        var regionSearch = document.getElementById("download-region-search");
        if (scope === "region") {
          if (regionSearch) regionSearch.classList.remove("hidden");
        } else {
          if (regionSearch) regionSearch.classList.add("hidden");
          currentDownloadRegion = null;
        }
        updateDownloadScopeText();
      });
    });
    document.querySelectorAll(".download-format-btn").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var format = this.dataset.format;
        var label = this.dataset.label || format;
        handleDownloadFormat(format, label);
      });
    });
    var regionInput = document.getElementById("download-region-input");
    if (regionInput) {
      regionInput.addEventListener("input", function () {
        var q = (this.value || "").trim().toLowerCase();
        var suggestionsEl = document.getElementById("download-region-suggestions");
        if (!suggestionsEl || !regionsData) return;
        suggestionsEl.innerHTML = "";
        if (!q) {
          suggestionsEl.classList.remove("visible");
          return;
        }
        var matches = [];
        for (var code in regionsData) {
          var r = regionsData[code];
          var searchNames = r.search_names || [];
          var displayName = r.display_name || code;
          var match = false;
          if (code.toLowerCase().indexOf(q) !== -1) match = true;
          if (displayName.toLowerCase().indexOf(q) !== -1) match = true;
          for (var i = 0; i < searchNames.length; i++) {
            if (searchNames[i].toLowerCase().indexOf(q) !== -1) {
              match = true;
              break;
            }
          }
          if (match) matches.push({ code: code, display: displayName });
        }
        if (matches.length > 0) {
          matches.slice(0, 10).forEach(function (m) {
            var item = document.createElement("button");
            item.type = "button";
            item.className = "download-region-item";
            item.textContent = m.display + " (" + m.code + ")";
            item.addEventListener("click", function () {
              currentDownloadRegion = m.code;
              regionInput.value = m.display + " (" + m.code + ")";
              suggestionsEl.classList.remove("visible");
            });
            suggestionsEl.appendChild(item);
          });
          suggestionsEl.classList.add("visible");
        } else {
          suggestionsEl.classList.remove("visible");
        }
      });
      regionInput.addEventListener("blur", function () {
        setTimeout(function () {
          var suggestionsEl = document.getElementById("download-region-suggestions");
          if (suggestionsEl) suggestionsEl.classList.remove("visible");
        }, 200);
      });
    }
  }

  function init() {
    createMap();
    loadDatasets();
    loadRegions();
    wireSearch();
    wireSearchToMap();
    wireDrawer();
    wirePreview();
    wireShareModal();
    wireDownloadModal();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
