Below is a **complete Markdown syntax reference** presented as a **single Markdown file** you can copy and save as
`Complete_Markdown_Syntax.md`.

---

````markdown
# Complete Markdown Syntax Guide
*A practical and comprehensive reference*

---

## 1. What is Markdown?

**Markdown** is a lightweight markup language used to format text using plain text symbols.  
It is widely used in:
- README files
- Documentation
- GitHub, GitLab, Bitbucket
- Blogs and static sites
- Jupyter notebooks

> This guide mainly follows **CommonMark + GitHub Flavored Markdown (GFM)**.

---

## 2. Headings

### ATX Style (Recommended)
```markdown
# Heading 1
## Heading 2
### Heading 3
#### Heading 4
##### Heading 5
###### Heading 6
````

### Setext Style

```markdown
Heading 1
=========

Heading 2
---------
```

---

## 3. Paragraphs & Line Breaks

```markdown
This is a paragraph.

This is another paragraph.
```

### Line Break

```markdown
Line one  
Line two
```

*(Two spaces before newline)*

---

## 4. Text Formatting

### Bold

```markdown
**Bold text**
__Bold text__
```

### Italic

```markdown
*Italic text*
_Italic text_
```

### Bold + Italic

```markdown
***Bold Italic***
```

### Strikethrough (GFM)

```markdown
~~Strikethrough~~
```

### Highlight (Not standard, HTML)

```markdown
<mark>Highlighted</mark>
```

---

## 5. Blockquotes

```markdown
> This is a blockquote
>> Nested blockquote
```

---

## 6. Lists

### Unordered List

```markdown
- Item
- Item
  - Sub-item
```

### Ordered List

```markdown
1. First
2. Second
3. Third
```

### Mixed List

```markdown
1. Item
   - Sub-item
   - Sub-item
```

---

## 7. Task Lists (GFM)

```markdown
- [x] Completed task
- [ ] Incomplete task
```

---

## 8. Code

### Inline Code

```markdown
Use `print()` to output text.
```

### Code Block

```markdown
```

code here

```
```

### Code Block with Language

````markdown
```python
print("Hello World")
````

````

---

## 9. Horizontal Rule

```markdown
---
***
___
````

---

## 10. Links

### Inline Link

```markdown
[OpenAI](https://openai.com)
```

### Reference Link

```markdown
[Google][1]

[1]: https://google.com
```

### Automatic Link

```markdown
<https://example.com>
```

---

## 11. Images

```markdown
![Alt text](image.png)
```

With title:

```markdown
![Alt text](image.png "Image Title")
```

---

## 12. Tables (GFM)

```markdown
| Name | Age | City |
|------|-----|------|
| Alice | 25 | NY |
| Bob | 30 | LA |
```

### Alignment

```markdown
| Left | Center | Right |
|:-----|:------:|------:|
| A | B | C |
```

---

## 13. Escaping Characters

Use `\` to escape formatting symbols:

```markdown
\*Not italic\*
\# Not a heading
```

---

## 14. HTML in Markdown

Markdown supports raw HTML:

```markdown
<div style="color:red">Red Text</div>
```

---

## 15. Footnotes (GFM / Extended)

```markdown
This is a sentence.[^1]

[^1]: This is the footnote.
```

---

## 16. Definition Lists (Extended)

```markdown
Term
: Definition
```

---

## 17. Emojis (GFM)

```markdown
:smile:
:rocket:
:fire:
```

😄 🚀 🔥

---

## 18. Comments (Invisible in Output)

```markdown
<!-- This is a comment -->
```

---

## 19. Table of Contents (Platform Dependent)

```markdown
- [Heading 1](#heading-1)
- [Heading 2](#heading-2)
```

(Some platforms auto-generate TOC)

---

## 20. Math (LaTeX – Supported on Some Platforms)

Inline:

```markdown
$E = mc^2$
```

Block:

```markdown
$$
a^2 + b^2 = c^2
$$
```

---

## 21. Admonitions (Platform-Specific)

```markdown
> **Note**
> This is important.
```

Some platforms support:

```markdown
:::warning
This is a warning
:::
```

---

## 22. Mermaid Diagrams (GFM)

````markdown
```mermaid
graph TD
A --> B
B --> C
````

````

---

## 23. Front Matter (YAML)

Used in static sites:
```markdown
---
title: Markdown Guide
author: You
date: 2025-01-01
---
````

---

## 24. Common Markdown Flavors

| Flavor         | Description              |
| -------------- | ------------------------ |
| CommonMark     | Standard Markdown        |
| GFM            | GitHub Flavored Markdown |
| Markdown Extra | Adds tables & footnotes  |
| RMarkdown      | Data science & reports   |

---

## 25. Best Practices

✔ Use consistent headings
✔ Leave blank lines between blocks
✔ Prefer fenced code blocks
✔ Avoid excessive HTML
✔ Follow CommonMark/GFM standards

---

## 26. Markdown Cheat Sheet (Quick)

```markdown
# Heading
**Bold**
*Italic*
~~Strike~~
`Code`
> Quote
- List
1. List
[Link](url)
![Image](img.png)
```

---

## 27. Summary

* Markdown is simple and powerful
* Easy to read and write
* Widely supported
* Perfect for documentation and notes

---

**End of Markdown File**

```

---

If you want, I can also:
- Create a **one-page Markdown cheat sheet**
- Add **GitHub-specific examples**
- Explain **Markdown vs HTML**
- Convert this into **PDF or slides**

Just tell me 👍
```
