Customer Transactions Spark Pipeline (Medallion Architecture)
Bu layihə müxtəlif kanallardan (POS, ECOM, ATM) gələn müştəri tranzaksiyalarını emal edən, təmizləyən, zənginləşdirən və risk analizini həyata keçirən kompleks Apache Spark (PySpark) məlumat boru kəməridir. Pipeline Medallion Arxitekturası (Bronze, Silver, Gold) əsasında qurulmuşdur və məlumatların S3/Minio obyekt anbarında və PostgreSQL verilənlər bazasında saxlanmasını təmin edir.

🛠 Texnologiyalar (Tech Stack)
Mühərrik: Apache Spark (PySpark)

Məlumat Formatı: Delta Lake, Parquet/CSV/JSON

Storage (Anbar): S3 / Minio

Verilənlər Bazası: PostgreSQL (JDBC)

🏗 Boru Kəmərinin Arxitekturası (Pipeline Architecture)
1. Spark Konfiqurasiyası
Delta & JDBC Dəstəyi: Delta Lake və PostgreSQL ilə əlaqə aktiv edilib.

Performans Optimizasiyası: Adaptive Query Execution (AQE) aktivdir, müvafiq Shuffle arakəsmələri (partitions) təyin edilib və kiçik ölçülü cədvəllər üçün Broadcast Join eşiyi tənzimlənib.

Storage & Yaddaş: S3/Minio uyğunluğu təmin edilib və partition overwrite mode dinamik (dynamic) olaraq təyin olunub.

2. Bronze Təbəqəsi (Xam Məlumatlar və Məlumat Keyfiyyəti)
Bu mərhələdə xam məlumatlar toplanır və ilkin yoxlamalardan keçirilir:

İngestion: POS, ECOM və ATM kanallarından 3 fərqli formatda gələn yalnız dünənki tranzaksiyalar /bronze/landing qovluğuna yüklənir.

Standartlaşdırma: Bütün mənbələr üçün channel və source_file (input_file_name() ilə) sütunları əlavə edilir. Məlumat tipləri (amount -> double/decimal, tx_ts -> timestamp) standartlaşdırılır və əskik sütunlar Null ilə əvəzlənərək vahid sxemə gətirilir. Dublikatlar silinir (ən sonuncu saxlanılır).

Məlumat Keyfiyyəti (Data Quality - DQ): * Qaydalar: tx_id != null, tx_ts != null, amount > 0, currency != null.

Yaxşı qeydlər /bronze/transactions_good, pis qeydlər isə uyğun xəta səbəbi ilə (dq_reason məsələn: tx_id_null;amount_invalid) /bronze/transactions_bad qovluğuna yazılır.

Ölçü (Dimension) Cədvəlləri: Postgres-dən customers, cards, mcc_mapping və fx_rate cədvəlləri oxunaraq Delta formatında /bronze/dims/ qovluğuna yüklənir.

3. Silver Təbəqəsi (Zənginləşdirmə və Təmizləmə)
Təmizlənmiş transactions_good məlumatları bu mərhələdə ölçü cədvəlləri ilə birləşdirilir və biznes məntiqi tətbiq edilir:

Sütunlardakı boşluqlar (trim) təmizlənir və böyük hərflərə (upper) çevrilir.

Birləşdirmələr (Joins): * Cards cədvəli vasitəsilə əskik customer_no bərpa edilir.

Customers cədvəli ilə müştərinin home_country (ana ölkəsi) tapılır.

mcc_mapping cədvəli Broadcast Join vasitəsilə birləşdirilərək risk çəkisi (risk weight) əlavə edilir.

fx_rate ilə məbləğlər (amount) rəsmi məzənnəyə əsasən AZN valyutasına çevrilir.

Biznes Bayraqları (Flags): Xarici tranzaksiyalar üçün is_foreign və risk eşiyini (>= 8) keçən tranzaksiyalar üçün is_high_risk bayraqları yaradılır.

4. Gold Təbəqəsi (Datamart və Risk Analizi)
Son mərhələdə Silver təbəqəsindəki məlumatlar müştəri bazında qruplaşdırılır (Daily Summary Datamart):

Aqreqasiyalar: Hər müştəri üçün ümumi tranzaksiya sayı, xərclənən məbləğ (AZN), xarici tranzaksiya sayı, yüksək riskli MCC tranzaksiyalarının sayı, fərqli ölkələrin sayı və ortalama xərclənən məbləğ (AZN) hesablanır.

Risk Skorunun Hesablanması: risk_score = (foreign_txn_count * 2) + (high_risk_txn_count * 3) + avg_risk_weight

Risk Səviyyəsi: LOW (<10), MEDIUM (10-19), HIGH (20+).

Yazma Prosesi: Partisiyalar coalesce edilərək azaldılır. Məlumat həm Delta formatında Gold qovluğuna, həm də JDBC Batch performansı ilə Postgres bazasına (public.customer_daily_risk) yazılır.
