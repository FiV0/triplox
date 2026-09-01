#!/usr/bin/env python3
"""
Convert an Obsidian markdown file to a Triplox (Astro + starlight-blog) blog post.

Usage:
    python obsidian-to-blog.py <path-to-obsidian-file>
    python obsidian-to-blog.py --date 2026-05-01 <path-to-obsidian-file>
"""

import sys
import re
import shutil
from datetime import datetime
from pathlib import Path


def slugify(text):
    """Convert text to a URL-friendly slug."""
    text = text.lower()
    text = re.sub(r'[\s_]+', '-', text)
    text = re.sub(r'[^\w\-]', '', text)
    text = re.sub(r'-+', '-', text)
    return text.strip('-')


def process_images(content, obsidian_path, assets_dir):
    """
    Find Obsidian wikilink images, copy them into public/assets/, and rewrite
    the references to standard markdown linking at /assets/<name>.
    """
    image_pattern = r'!\[\[([^\]]+\.(?:png|jpg|jpeg|gif|svg|webp))\]\]'
    matches = re.findall(image_pattern, content, re.IGNORECASE)

    source_images_dir = obsidian_path.parent / 'images'
    copied_images = []

    for image_name in matches:
        source_image = source_images_dir / image_name
        dest_image = assets_dir / image_name

        if source_image.exists():
            shutil.copy2(source_image, dest_image)
            copied_images.append(image_name)
        else:
            print(f"  Warning: Image not found: {source_image}")

    def replace_image(match):
        image_name = match.group(1)
        alt_text = Path(image_name).stem
        return f'![{alt_text}](/assets/{image_name})'

    content = re.sub(image_pattern, replace_image, content, flags=re.IGNORECASE)
    return content, copied_images


def transform_urls(content):
    """Wrap bare URLs in angle brackets so they render as links."""
    url_pattern = r'(?<!\]\()(?<![<\[])https?://[^\s\)\]>]+'

    def make_link(match):
        return f'<{match.group(0)}>'

    return re.sub(url_pattern, make_link, content)


def find_post_slug(note_name, posts_dir):
    """
    Look for a matching blog post in posts_dir and return its slug
    (filename without extension), or None if not found.
    """
    slug = slugify(note_name)
    for post_file in posts_dir.glob('*.md'):
        if slug == post_file.stem or slug in post_file.stem:
            return post_file.stem
    return None


def find_doc_slug(note_name, docs_dir):
    """
    Look for a matching docs page anywhere under docs_dir and return its
    URL path (relative to docs_dir, without extension), or None.
    """
    slug = slugify(note_name)
    for ext in ('*.md', '*.mdx'):
        for doc_file in docs_dir.rglob(ext):
            if slug == doc_file.stem:
                rel = doc_file.relative_to(docs_dir).with_suffix('')
                return str(rel)
    return None


def protect_code_blocks(content):
    """Replace fenced and inline code with placeholders so transforms skip them."""
    code_blocks = {}
    counter = [0]

    def save_fenced(match):
        placeholder = f"___CODE_BLOCK_{counter[0]}___"
        code_blocks[placeholder] = match.group(0)
        counter[0] += 1
        return placeholder

    protected = re.sub(r'```[^\n]*\n.*?```', save_fenced, content, flags=re.DOTALL)

    def save_inline(match):
        placeholder = f"___INLINE_CODE_{counter[0]}___"
        code_blocks[placeholder] = match.group(0)
        counter[0] += 1
        return placeholder

    protected = re.sub(r'`[^`]+`', save_inline, protected)
    return protected, code_blocks


def restore_code_blocks(content, code_blocks):
    for placeholder, code in code_blocks.items():
        content = content.replace(placeholder, code)
    return content


def resolve_link(note_name, posts_dir, docs_dir):
    """Return a URL for a note name, preferring blog posts over docs pages."""
    post_slug = find_post_slug(note_name, posts_dir)
    if post_slug:
        return f'/blog/{post_slug}/'
    doc_slug = find_doc_slug(note_name, docs_dir)
    if doc_slug:
        return f'/{doc_slug}/'
    return None


def transform_wikilinks(content, posts_dir, docs_dir):
    """
    Convert Obsidian wikilinks:
      [[#Header]]              -> [Header](#header)
      [[#Header|display]]      -> [display](#header)
      [[note#section|display]] -> [display](url#section)
      [[note#section]]         -> [section](url#section)
      [[note|display]]         -> [display](url)
      [[note]]                 -> [note](url)
    Falls back to plain text if no matching post or doc is found.
    """
    protected, code_blocks = protect_code_blocks(content)

    def header_with_display(match):
        header, display = match.group(1), match.group(2)
        return f'[{display}](#{slugify(header)})'

    protected = re.sub(r'\[\[#([^\]|]+)\|([^\]]+)\]\]', header_with_display, protected)

    def header_only(match):
        header = match.group(1)
        return f'[{header}](#{slugify(header)})'

    protected = re.sub(r'\[\[#([^\]]+)\]\]', header_only, protected)

    def post_section_display(match):
        note, section, display = match.group(1), match.group(2), match.group(3)
        url = resolve_link(note, posts_dir, docs_dir)
        if url:
            return f'[{display}]({url}#{slugify(section)})'
        return display

    protected = re.sub(r'\[\[([^#\]|]+)#([^\]|]+)\|([^\]]+)\]\]', post_section_display, protected)

    def post_section(match):
        note, section = match.group(1), match.group(2)
        url = resolve_link(note, posts_dir, docs_dir)
        if url:
            return f'[{section}]({url}#{slugify(section)})'
        return f'{note}#{section}'

    protected = re.sub(r'\[\[([^#\]|]+)#([^\]|]+)\]\]', post_section, protected)

    def post_display(match):
        note, display = match.group(1), match.group(2)
        url = resolve_link(note, posts_dir, docs_dir)
        if url:
            return f'[{display}]({url})'
        return display

    protected = re.sub(r'\[\[([^\]|#]+)\|([^\]]+)\]\]', post_display, protected)

    def post_only(match):
        note = match.group(1)
        url = resolve_link(note, posts_dir, docs_dir)
        if url:
            return f'[{note}]({url})'
        return note

    protected = re.sub(r'\[\[([^\]|#]+)\]\]', post_only, protected)

    return restore_code_blocks(protected, code_blocks)


def transform_display_math(content):
    """Ensure $$...$$ display math sits on its own lines."""
    def ensure_block_math(match):
        return '\n\n' + match.group(0) + '\n\n'

    content = re.sub(r'\$\$[^$]+\$\$', ensure_block_math, content)
    content = re.sub(r'\n{3,}', '\n\n', content)
    return content


def yaml_escape(value):
    """Quote a YAML scalar if it contains characters that would otherwise need escaping."""
    if re.search(r'[:#\-\[\]\{\}&*!|>\'\"%@`,]', value) or value != value.strip():
        return '"' + value.replace('\\', '\\\\').replace('"', '\\"') + '"'
    return value


def create_blog_post(obsidian_file_path, custom_date=None, author_name=None, author_title=None):
    obsidian_path = Path(obsidian_file_path)

    if not obsidian_path.exists():
        print(f"Error: File not found: {obsidian_file_path}")
        sys.exit(1)

    if obsidian_path.suffix != '.md':
        print(f"Error: File must be a markdown file (.md): {obsidian_file_path}")
        sys.exit(1)

    with open(obsidian_path, 'r', encoding='utf-8') as f:
        content = f.read()

    title = obsidian_path.stem
    post_date = custom_date if custom_date else datetime.now()
    date_str = post_date.strftime('%Y-%m-%d')
    slug = slugify(title)

    script_dir = Path(__file__).parent
    posts_dir = script_dir / 'src' / 'content' / 'docs' / 'blog'
    docs_dir = script_dir / 'src' / 'content' / 'docs'
    assets_dir = script_dir / 'public' / 'assets'

    posts_dir.mkdir(parents=True, exist_ok=True)
    assets_dir.mkdir(parents=True, exist_ok=True)

    output_path = posts_dir / f'{slug}.md'

    content, copied_images = process_images(content, obsidian_path, assets_dir)
    content = transform_wikilinks(content, posts_dir, docs_dir)
    content = transform_urls(content)
    content = transform_display_math(content)

    name = author_name or 'Finn Völkel'

    author_lines = f'  - name: {yaml_escape(name)}\n'
    if author_title:
        author_lines += f'    title: {yaml_escape(author_title)}\n'

    front_matter = (
        '---\n'
        f'title: {yaml_escape(title)}\n'
        f'date: {date_str}\n'
        'authors:\n'
        f'{author_lines}'
        '---\n\n'
    )

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(front_matter + content)

    print('Successfully created blog post:')
    print(f'  File: {output_path}')
    print(f'  Title: {title}')
    print(f'  Date: {date_str}')
    print(f'  URL:  /blog/{slug}/')
    if copied_images:
        print(f'  Copied images: {", ".join(copied_images)}')

    return output_path


def main():
    args = sys.argv[1:]

    if not args or args[0] in ('-h', '--help'):
        print('Usage: python obsidian-to-blog.py [options] <path-to-obsidian-file>')
        print('\nOptions:')
        print('  --date YYYY-MM-DD   Override the post date (default: today)')
        print('  --author NAME       Author name (default: "Finn Völkel")')
        print('  --title TITLE       Author title (default: none)')
        print('\nExample:')
        print("  python obsidian-to-blog.py '/home/user/obsidian/My Post.md'")
        print("  python obsidian-to-blog.py --date 2026-05-01 'My Post.md'")
        sys.exit(0 if args else 1)

    custom_date = None
    author_name = None
    author_title = None
    positional = []

    i = 0
    while i < len(args):
        arg = args[i]
        if arg == '--date':
            i += 1
            try:
                custom_date = datetime.strptime(args[i], '%Y-%m-%d')
            except (IndexError, ValueError):
                print('Error: --date expects YYYY-MM-DD')
                sys.exit(1)
        elif arg == '--author':
            i += 1
            author_name = args[i] if i < len(args) else None
        elif arg == '--title':
            i += 1
            author_title = args[i] if i < len(args) else None
        else:
            positional.append(arg)
        i += 1

    if not positional:
        print('Error: missing path to Obsidian markdown file')
        sys.exit(1)

    create_blog_post(
        positional[0],
        custom_date=custom_date,
        author_name=author_name,
        author_title=author_title,
    )


if __name__ == '__main__':
    main()
