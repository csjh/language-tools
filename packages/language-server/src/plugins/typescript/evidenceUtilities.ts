import { existsSync, readFileSync } from 'fs';
import { dirname, join } from 'path';
import { parse } from 'yaml';
import path from 'path';

export function getEvidencePlugins(file: string) {
    const root = getProjectRoot(file);
    if (!root) return {};
    return parse(readFileSync(join(root, 'evidence.plugins.yaml'), 'utf-8'));
}

export function getRenderedFiles(root: string) {
    const file = readFileSync(path.join(root, 'static', 'data', 'manifest.json'), 'utf-8');
    const { renderedFiles }: { renderedFiles: Record<string, string[]> } = JSON.parse(file);

    return Object.fromEntries(
        Object.entries(renderedFiles).map(([source, filepaths]) => [
            source,
            filepaths.map((filepath) => path.resolve(path.join('.', 'static', filepath)))
        ])
    );
}

const cache = new Map<string, string>();
export function getProjectRoot(file: string | undefined): string | null {
    if (!file) return null;

    const cached = cache.get(dirname(file));
    if (cached) {
        return cached;
    }

    let currentDir = file;
    let nextDir = dirname(file);
    while (currentDir !== nextDir) {
        currentDir = nextDir;
        const config = join(currentDir, `evidence.plugins.yaml`);
        if (existsSync(config)) {
            cache.set(dirname(file), currentDir);
            return currentDir;
        }
        nextDir = dirname(currentDir);
    }

    return null;
}
