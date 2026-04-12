locals {
  buckets = {
    landing = "${var.project}-landinglayer-${var.environment}"
    bronze  = "${var.project}-bronzelayer-${var.environment}"
    silver  = "${var.project}-silverlayer-${var.environment}"
    gold    = "${var.project}-goldlayer-${var.environment}"
  }
  folders = [
    "customers",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "sellers",
    "product_category_translation",
    "geolocation"
  ]
}

resource "aws_s3_bucket" "data_lake" {
  for_each = local.buckets

  bucket = each.value

  tags = {
    Name        = "Data Lake - ${each.key}"
    Environment = var.environment
    Layer       = each.key
  }
}


resource "aws_s3_object" "prefixes" {
  for_each = {
    for combo in setproduct(keys(local.buckets), local.folders) :
    "${combo[0]}-${combo[1]}" => {
      layer  = combo[0]
      folder = combo[1]
    }
  }

  bucket = aws_s3_bucket.data_lake[each.value.layer].id
  key    = "raw/${each.value.folder}/"
}