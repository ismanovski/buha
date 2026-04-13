async function reApiFetch(path, options = {}) {
  const response = await fetch(path, {
    headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
    ...options,
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`API ${path} failed: ${response.status} ${text}`);
  }

  if (response.status === 204) return null;
  return response.json();
}

async function saveReProject(project) {
  const now = new Date().toISOString();
  const payload = {
    createdAt: project.createdAt || now,
    updatedAt: now,
    ...project,
  };
  return reApiFetch('/re/api/projects', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

async function listReProjects() {
  const data = await reApiFetch('/re/api/projects');
  return data.projects || [];
}

async function getReProject(id) {
  try {
    return await reApiFetch(`/re/api/projects/${encodeURIComponent(id)}`);
  } catch {
    return null;
  }
}

async function deleteReProject(id) {
  await reApiFetch(`/re/api/projects/${encodeURIComponent(id)}`, {
    method: 'DELETE',
  });
}

async function listReSuggestions() {
  const data = await reApiFetch('/re/api/suggestions');
  return data.suggestions || [];
}

async function saveReSuggestions(suggestions) {
  const payload = Array.isArray(suggestions) ? suggestions : [];
  await reApiFetch('/re/api/suggestions', {
    method: 'POST',
    body: JSON.stringify({ suggestions: payload }),
  });
}
