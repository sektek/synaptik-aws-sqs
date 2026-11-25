const MINOR = 1;
const PATCH = 2;

const whatBump = commits => {
  let level = null;

  for (const commit of commits) {
    if (commit.notes.length > 0 || commit.type === 'feat') {
      return { level: MINOR, reason: 'feature or breaking change' };
    }

    if (['fix', 'perf', 'revert'].includes(commit.type)) {
      level = PATCH;
    }
  }

  return { level, reason: level === null ? 'no releasable commits' : 'fix' };
};

export default {
  plugins: {
    '@release-it/conventional-changelog': {
      preset: {
        name: 'conventionalcommits',
      },
      whatBump,
      infile: 'CHANGELOG.md',
    },
  },
  git: {
    commitMessage: 'chore: release v${version} [skip ci]',
    tagName: 'v${version}',
    push: true,
    pushArgs: ['--follow-tags'],
  },
  github: {
    release: true,
    releaseName: 'v${version}',
  },
  npm: {
    publish: false,
  },
};
