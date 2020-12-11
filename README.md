# Задание 5 (Таблица редиректов)

With cool solutions:
- task1 by plain SQL only (use window function)
- task2 by extremly thin recursion (stack overflow prevented)

## Условие
```ruby
# Дана следующая модель и миграция от этой модели

  create_table "deleted_urls", force: true do |t|
    t.string   "prev_url"
    t.string   "next_url"
    t.datetime "created_at"
  end

  add_index "deleted_urls", ["prev_url"], name: "index_deleted_urls_on_prev_url", unique: true, using: :btree

# Эта модель отвечает за создание редиректов на существующие ссылки в проекте при смене или удалении ссылок в проекте.
# Пример, при попадании на урл prev_url в роутинге, осуществляется редикт на next_url.
# При реализации решения, нужно учитывать, что бд содержит около 500_000 записей.

# Еще примеры.

# Пример 1, была ссылка вида A, которая отвечала за показ объявления, однако, объявление было удалено.
# Чтобы не получать 404 ошибку при заходе по ссылке A, создается редирект на раздел по ссылке B. 
# Пользователь, зайдя на страницу A, теперь будет сразу попадать на страницу B.

# Пример 2, была ссылка вида A, которая отвечала за показ объявления, однако, объявление было изменено и поменялся урл у этого объявления на B.
# Чтобы не получаеть 404 ошибку при заходе по старой ссылке A, создается редирект на новую ссылку B. 
# Пользователь, зайдя на страницу A, теперь будет сразу попадать на страницу B.

# Задачи 

# 1) Необходимо написать rake задачу, которая бы удалила циклические редиректы из таблицы deleted_urls.
# Пример циклического редиректа. Пусть A, B - это ссылки. 
# В БД есть запись deleted_urls(prev_url: A, next_url: B, created_at: created_at1) и deleted_urls(prev_url: B, next_url: A, created_at: created_at2), где created_at1 < created_at2.
# Циклический редирект удалится, если удалить запись в БД deleted_urls, образующию петлю из ссылок, с наименьшим created_at

# 2) Необходимо написать rake задачу, которая бы нашла и изменила определенные записи таблицы deleted_urls следующим образом.
# Записи, которые должны быть изменены, должны удовлетворять требованию, что количество редиректов после посещения при редиректе текущей записи на последующие записи больше N. Где N > 10.
# Записи, которые должны быть изменены, должны ссылаться на последнюю хвостовую запись, то есть next_url, изменяемых записей, должен стать равен next_url хвостовой записи.

# Дополнительный материaл https://www.google.com/search?q=%D1%86%D0%B8%D0%BA%D0%BB%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B8%D0%B9+%D1%80%D0%B5%D0%B4%D0%B8%D1%80%D0%B5%D0%BA%D1%82
```

## Smoke
### install it
```bash
git clone https://github.com/dmitryck/redirect_table.git
cd redirect_table
bundle
```
### configure it
Create `.env` file with your config (see [.env.example](https://github.com/dmitryck/redirect_table/blob/master/.env.example))
### run tasks
_notice_: you must have your `deleted_urls` table with data
```
rake task1
rake task2
```

## Code
```ruby
# Rakefile

require 'active_record'
require 'dotenv/load'

#
# ORM
#
ActiveRecord::Base.establish_connection \
  adapter:  'postgresql',
  host:     ENV['DBHOST'],
  username: ENV['DBUSER'],
  password: ENV['DBPASS'],
  database: ENV['DBNAME']

class Table < ActiveRecord::Base
  self.table_name = 'deleted_urls'
end

#
# tasks
#
desc 'Remove cycle links from table'
task :task1 do
  ActiveRecord::Base.connection.execute(<<-SQL)
    delete from deleted_urls
    where id in (
      select id
      from (
        select t2.id, link1, link2
        from (
          select id, link as link1, lag(id) over() target_id, lag(link) over() link2
          from (
            select id, (prev_url||next_url) as link, created_at
            from deleted_urls
            union (
              select id, (next_url||prev_url) as link, created_at
              from deleted_urls
            )
            order by link, created_at desc
          ) t1
        ) t2
        where t2.link1 = t2.link2
      ) t3
    )
  SQL
end

desc 'Replace cross links in table by last value'
task :task2 do
  RubyVM::InstructionSequence.compile_option = {
    tailcall_optimization: true,
    trace_instruction: false
  }

  #
  # alternative exit from recursion without rescue
  # fast variant too:
  # url, id = data.delete(url || (data.first&.send(:[], 0) || return))
  #
  # slow classic variant (add at method begining):
  # return if data.count == 0
  #
  RubyVM::InstructionSequence.new(<<-EOS).eval
    def worker(data, url, ids, result)
      url, id = data.delete(url || data.first[0]) rescue return
      data[url] ? ids << id : (result[url] = ids if ids.count > 10; ids = [])
      worker(data, url, ids, result)
    end
  EOS

  Table.find_each.each_with_object(data = {}) do |row, hash|
    hash[row.prev_url] = [row.next_url, row.id]
  end

  worker(data, nil, [], result = {})

  for url, ids in result
    Table.where(id: ids).update_all(next_url: url)
  end
end
```
See [Rakefile](https://github.com/dmitryck/redirect_table/blob/master/Rakefile)

No tests version
